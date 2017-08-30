#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <string.h>

#define BUFSIZE 256
#define NANO_TO_S 1000000000
#define bool int
#define true 1
#define false 0
enum
{
	C = 0, /*create */
	D,     /* delete */
	R,     /* read */
	W,     /* write */
	A,     /* append */
	U,     /* update */
	S,     /* sleep */
	F,     /* fsync */
};

/* global paramters */
int g_create_bias;
int g_rw_bias;
int g_append_bias;
int g_min_iosize;
int g_max_iosize;
int g_sleep;
int g_burst;
int g_fsync_freq;
int g_num_files;
int g_num_dirs;
int g_min_filesize;
int g_max_filesize;
int g_num_ops;
int g_min_time;
int g_seed;
char *g_target_dir;
int g_num_threads;
const int num_args = 17;

int *next_fileno_array;
char *io_buf;
char *fill_buf;
#define FILLBUFSIZE 1024
static const char randomchars[] = "#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";

long *burst_latency;
long g_bytes_written;
long g_bytes_read;
int g_files_created;
int g_files_deleted;
int g_files_written; /* # of write ops, both append and update */
int g_files_read; /* # of read ops */

void cleanup();

/* TODO
 * -switches for seq vs rand i/o? updates are always rand at the moment, 
 *  but could be left alone to use the curr offset. reads are sequential, using
 *  just the curr offset
 * -switch for direct io option. needs to be aligned
 * -file size distribution skew
 * -i/o size distribution skew
 *  
 */
void usage()
{
	/* ops: create/delete,  read/write [append/update]
	 * i/o min size, i/o max size, sleeptime, burst size, fsync frequency
	 * number of files, number of directories, min file size, max file size
	 * number of operations, max time limit
	 * rand seed
	 * target directory
	 * number of threads
	 */
	printf("./spiobench create-delete-bias read-write-bias append-update-bias burst-size fsync-frequency(seconds) number-of-files-per-dir number-of-dirs filesize-min filesize-max max-number-ops min-runtime(seconds) rand-seed absolute-target-dir threads\n"
			);
	printf("bias values are percentages 0-100\n");
}

void print_test_params()
{
	printf("create-delete-bias: %d\n", g_create_bias);
	printf("read-write-bias: %d\n", g_rw_bias);
	printf("append-update-bias: %d\n", g_append_bias);
	printf("i/o size range: %d %d\n", g_min_iosize, g_max_iosize);
	printf("sleep time: %d\n", g_sleep);
	printf("burst size: %d\n", g_burst);
	printf("fsync freq: %d\n", g_fsync_freq);
	printf("number of files per dir: %d\n", g_num_files);
	printf("number of dirs: %d\n", g_num_dirs);
	printf("file size range: %d %d\n", g_min_filesize, g_max_filesize);
	printf("max number of ops: %d\n", g_num_ops);
	printf("min runtime: %d\n", g_min_time);
	printf("rand seed %d\n", g_seed);
	printf("target dir: %s\n", g_target_dir);
	printf("number of threads: %d\n", g_num_threads);
}

bool validate_params()
{
	if (g_create_bias < 0 || g_create_bias > 100)
	{
		printf("create-delete-bias is out of range: %d\n", g_create_bias);
		return false;
	}
	if (g_rw_bias < 0 || g_rw_bias > 100)
	{
		printf("read-write-bias is out of range: %d\n", g_rw_bias);
		return false;
	}
	if (g_append_bias < 0 || g_append_bias > 100)
	{
		printf("append-update-bias is out of range: %d\n", g_append_bias);
		return false;
	}
	if (g_min_iosize < 0 || g_min_iosize > g_max_iosize)
	{
		printf("min i/o size is out of range: %d (max is %d)\n", g_min_iosize, g_max_iosize);
		return false;
	}
	if (g_sleep < 0)
	{
		printf("sleep time is out of range: %d\n", g_sleep);
		return false;
	}
	if (g_burst < 0)
	{
		printf("burst size is out of range: %d\n", g_burst);
		return false;
	}
	if (g_fsync_freq < 0)
	{
		printf("fsync freq is out of range: %d\n", g_fsync_freq);
		return false;
	}
	if (g_num_files < 0)
	{
		printf("number of files is out of range: %d\n", g_num_files);
		return false;
	}
	if (g_num_dirs < 0)
	{
		printf("number of dirs is out of range: %d\n", g_num_dirs);
		return false;
	}
	if (g_min_filesize < 0 || g_min_filesize > g_max_filesize)
	{
		printf("min file size is out of range: %d (max is %d)\n", g_min_filesize, g_max_filesize);
		return false;
	}
	if (g_num_ops < 0)
	{
		printf("number of ops is out of range: %d\n", g_num_ops);
		return false;
	}
	if (g_min_time < 0)
	{
		printf("min runtime is out of range: %d\n", g_min_time);
		return false;
	}
	if (g_num_threads < 0)
	{
		printf("number of threads is out of range: %d\n", g_num_threads);
		return false;
	}

	return true;
}

void generate_rand_buf(char * buf, int len)
{
	int i;
	for (i = 0; i < len/sizeof(int); i++)
	{
		((int *)buf)[i] = rand();
	}
	for (i = 0; i < len%sizeof(int); i++)
	{
		buf[len-len%sizeof(int)+i] = randomchars[rand()%sizeof(randomchars)];
	}
}

long diff_time (struct timespec start, struct timespec stop)
{
	return (stop.tv_sec-start.tv_sec)*NANO_TO_S + stop.tv_nsec - start.tv_nsec;
}

void emit_time(int type, long time)
{
	switch (type)
	{
		case C:
			printf("C: %ld\n", time);
			break;	
		case D:
			printf("D: %ld\n", time);
			break;
		case R:
			printf("R: %ld\n", time);
			break;
		case W:
			printf("W: %ld\n", time);
			break;
		case A:
			printf("A: %ld\n", time);
			break;
		case U:
			printf("U: %ld\n", time);
			break;
		case S:
			printf("S: %d\n", g_sleep);
			break;
		case F:
			printf("F: %ld\n", time);
			break;
		default:
			printf("invalid type: %ld\n", time);
	}
}

bool create_dirs()
{
	/* just name the dirs in numerical order */
	int i;
	int ret;
	char buf[BUFSIZE];

	sprintf(buf, "%s", g_target_dir);
	printf("making target dir %s\n", buf);
	ret = mkdir(buf, 0700);
	if (ret)
	{
		perror("create_dirs failed\n");
	}
	for (i = 0; i < g_num_dirs; i++)
	{
		sprintf(buf, "%s/%d", g_target_dir, i);
		printf("making target dir %s\n", buf);
		ret = mkdir(buf, 0700);
		if (ret)
		{
			perror("create_dirs failed\n");
		}
	}
	return true;
}

bool create_a_file(int dirno)
{
	char buf[BUFSIZE];
	int fd;
	int filesize; //, bytes_written;

	sprintf(buf, "%s/%d/%d", g_target_dir, dirno, next_fileno_array[dirno]);
	fd = open(buf, O_CREAT|O_RDWR, 0644);

	if (-1 == fd)
	{
		perror("create_a_file failed\n");
		return false;
	}
	g_files_created++;

	filesize = (rand()%(g_max_filesize - g_min_filesize + 1)) + g_min_filesize;
/*
	bytes_written = 0;
	while (filesize - bytes_written < FILLBUFSIZE)
	{
		generate_rand_buf(fill_buf, FILLBUFSIZE);
		write(fd, fill_buf, FILLBUFSIZE);
		bytes_written += FILLBUFSIZE;
		g_bytes_written += FILLBUFSIZE;
		g_files_written++;
	}
	while (bytes_written < filesize)
	{
		generate_rand_buf(fill_buf, 1);
		write(fd, fill_buf, 1);
		bytes_written++;
		g_bytes_written += 1;
		g_files_written++;
	}
*/
	generate_rand_buf(fill_buf, filesize);
	write(fd, fill_buf, filesize);
	g_bytes_written += filesize;
	g_files_written++;

	close(fd);

	next_fileno_array[dirno]++;

	return true;
}

bool create_a_file_fast(int dirno, char *buf, int len)
{
	/* the buffer is already randomly filled */
	int fd;

	sprintf(buf, "%s/%d/%d", g_target_dir, dirno, next_fileno_array[dirno]);
	fd = open(buf, O_CREAT|O_RDWR, 0644);

	if (-1 == fd)
	{
		perror("create_a_file failed\n");
		return false;
	}
	g_files_created++;

/*
	bytes_written = 0;
	while (filesize - bytes_written < FILLBUFSIZE)
	{
		generate_rand_buf(fill_buf, FILLBUFSIZE);
		write(fd, fill_buf, FILLBUFSIZE);
		bytes_written += FILLBUFSIZE;
		g_bytes_written += FILLBUFSIZE;
		g_files_written++;
	}
	while (bytes_written < filesize)
	{
		generate_rand_buf(fill_buf, 1);
		write(fd, fill_buf, 1);
		bytes_written++;
		g_bytes_written += 1;
		g_files_written++;
	}
*/
	write(fd, buf, len);
	g_bytes_written += len;
	g_files_written++;

	close(fd);

	next_fileno_array[dirno]++;

	return true;
}

bool create_files(int dirno)
{
	/* just name the files in numerical order */
	int j;
	int ret;

	for (j = 0; j < g_num_files; j++)
	{
		ret = create_a_file(dirno);	
		if (!ret)
			return false;
		if (0 == j%100)
			printf("created %d of %d files\n", j, g_num_files);
	}

	return true;

}

struct dirent * select_rand_file(int dirno)
{

	int i, fileno, file_cnt;
	DIR * dir;
	struct dirent *de;
	char buf[BUFSIZE];

	
	sprintf(buf, "%s/%d", g_target_dir, dirno);

	dir = opendir(buf);

	if (NULL == dir)
	{
		perror("unable to open dir for file select\n");
		cleanup();
		exit(-1);
	}

	file_cnt = 0;
	de = readdir(dir);
	while (NULL != de)
	{
//		printf("%s\n", de->d_name);
		file_cnt++;
		de = readdir(dir);
	}
	if (2 == file_cnt)
	{
		/* this dir is empty */
		printf("%s: dir is empty. re-run with more files or fewer ops or different bias\n", buf);
		cleanup();
		exit(-1);
	}

retry:
	fileno = rand()%(file_cnt);
	i = 0;
//	printf("%s: selectinf file %d of %d\n", buf, fileno, file_cnt);
	rewinddir(dir);
	de = readdir(dir);
	while (NULL != de && i != fileno)
	{
		de = readdir(dir);
		i++;
	}
	if( (0 == strcmp(de->d_name, "..")) || (0 == strcmp(de->d_name, ".")))
		goto retry;

	closedir(dir);
	return de;

}

void delete_a_file(int dirno)
{

	struct dirent *de;
	char buf[BUFSIZE];

	de = select_rand_file(dirno);

	if (NULL == de)
	{
		printf("not enough files in this directory to delete. rerun with more files\n");
		cleanup();
		exit(-1);	
	}

	sprintf(buf, "%s/%d/%s", g_target_dir, dirno, de->d_name);
	remove(buf);
	g_files_deleted++;

}

void delete_files(int dirno)
{
	struct dirent *de;
	char buf[BUFSIZE];
	DIR * dir;

	sprintf(buf, "%s/%d", g_target_dir, dirno);

	dir = opendir(buf);

	if (NULL == dir)
	{
		perror("unable to open dir for all files deletion\n");
		return;
	}

	de = readdir(dir);
	while (NULL != de)
	{
		sprintf(buf, "%s/%d/%s", g_target_dir, dirno, de->d_name);
		remove(buf);
		g_files_deleted++;
	}

	closedir(dir);
}

void delete_dirs()
{
	int i;
	int ret;
	char buf[BUFSIZE];

	for (i = 0; i < g_num_dirs; i++)
	{
		sprintf(buf, "%s/%d", g_target_dir, i);
		ret = rmdir(buf);
		if (ret)
		{
			perror("delete_dirs failed\n");
		}
	}
}

void cleanup()
{
//TODO
}

void do_work()
{
	/* select an operation according to bias:
	 * create/delete
	 * read/write (append/update)
	 * what is the bias between create/delete and file i/o??
	 * postmark defines a transaction which does both 
	 */

	struct timespec start, stop;
	int dirno, fd, ret, io_size, tmp;
	struct dirent *de;
	struct stat file_stat;
	char buf[BUFSIZE];

	/* select a rand dir to do a create/delete */
	dirno = rand()%g_num_dirs;
	if (0 != g_create_bias && rand()%100 <= g_create_bias)
	{
		/*create a file*/
		io_size = (rand()%(g_max_filesize - g_min_filesize + 1)) + g_min_filesize;
		generate_rand_buf(fill_buf, io_size);
		clock_gettime(CLOCK_MONOTONIC, &start);
		create_a_file_fast(dirno, fill_buf, io_size);
		clock_gettime(CLOCK_MONOTONIC, &stop);
		emit_time(C, diff_time(start, stop));
	}
	else
	{
		/*select a file to delete*/
		clock_gettime(CLOCK_MONOTONIC, &start);
		delete_a_file(dirno);
		clock_gettime(CLOCK_MONOTONIC, &stop);
		emit_time(D, diff_time(start, stop));
	}

	/*select a rand dir to a an i/o*/
	dirno = rand()%g_num_dirs;
	de = select_rand_file(dirno);
	if (NULL == de)
	{
		printf("not enough files to do i/o. rerun with more files\n");
		cleanup();
		exit(-1);
	}

	sprintf(buf, "%s/%d/%s", g_target_dir, dirno, de->d_name);
	fd = open(buf, O_RDWR);
	if (-1 == fd)
	{
		printf("attempt %s\n", buf);
		perror("unable to open file to do i/o\n");
		return;
	}

	io_size = rand()%(g_max_iosize - g_min_iosize +1) + g_min_iosize;
	if (0 != g_rw_bias && rand()%100 <= g_rw_bias)
	{
		/* do a read*/
		/* note that we may read fewer bytes that desired. make a note of this? */
		/* this will also be a sequential read */
		clock_gettime(CLOCK_MONOTONIC, &start);
		ret = read(fd, io_buf, io_size);
		if (-1 == ret)
		{
			perror("read incomplete\n");
			cleanup();
			exit(-1);
		} else if (ret < io_size)
		{
			/* we read to the end of the file. there's a good chance the
			 * previous reads are still in page cache. Do we care?
			 */
			lseek(fd, 0, SEEK_SET);
		}
		clock_gettime(CLOCK_MONOTONIC, &stop);
		emit_time(R, diff_time(start, stop));
		g_bytes_read += io_size;
		g_files_read++;

	}
	else {
		if (0 != g_append_bias && rand()%100 <= g_append_bias)
		{
			/* do an append */
			generate_rand_buf(io_buf, io_size);
			clock_gettime(CLOCK_MONOTONIC, &start);
			if( -1 == lseek(fd, 0, SEEK_END))
			{
				perror("lseek append\n");
				cleanup();
				exit(-1);
			}
			ret = write(fd, io_buf, io_size);
			if (ret != io_size)
			{
				perror("append incomplete\n");
				cleanup();
				exit(-1);
			}
			clock_gettime(CLOCK_MONOTONIC, &stop);
			emit_time(A, diff_time(start, stop));
			g_bytes_written += io_size;
			g_files_written++;
			}
		else
		{
				/* do in-place update */
			generate_rand_buf(io_buf, io_size);
			if (-1 == fstat(fd, &file_stat))
			{
				perror("stat\n");
				cleanup();
				exit(-1);
			}
			tmp = rand()%(file_stat.st_size-io_size);
			if (-1 == lseek(fd, tmp, SEEK_SET))
			{
				printf("file size: %ld seek: %d io size: %d\n", file_stat.st_size, tmp, io_size);
				perror("lseek update\n");
				cleanup();
				exit(-1);
			}
			clock_gettime(CLOCK_MONOTONIC, &start);
			ret = write(fd, io_buf, io_size);
			if (ret != io_size)
			{
				perror("update incomplete\n");
				cleanup();
				exit(-1);
			}
			clock_gettime(CLOCK_MONOTONIC, &stop);
			emit_time(U, diff_time(start, stop));
			g_bytes_written += io_size;
			g_files_written++;
		}
	}
	close(fd);
}

void print_stats(long elapsed)
{
	printf("files created: %d\n", g_files_created);
	printf("--per second: %f\n", (double)g_files_created/(double)elapsed*NANO_TO_S);
	printf("files deleted: %d\n", g_files_deleted);
	printf("--per second: %f\n", (double)g_files_deleted/(double)elapsed*NANO_TO_S);
	printf("files written: %d\n", g_files_written);
	printf("--per second: %f\n", (double)g_files_written/(double)elapsed*NANO_TO_S);
	printf("files read: %d\n", g_files_read);
	printf("--per second: %f\n", (double)g_files_read/(double)elapsed*NANO_TO_S);
	printf("bytes written %ld\n", g_bytes_written);
	printf("write throughput MB/s: %f\n", ((double)g_bytes_written/1024.0/1024.0)/((double)elapsed/NANO_TO_S));
	printf("bytes read %ld\n", g_bytes_read);
	printf("read throughput MB/s: %f\n", ((double)g_bytes_read/1024.0/1024.0)/((double)elapsed/NANO_TO_S));
}

int main(int argc, char **argv)
{
	int i;
	struct timespec start, stop, g_start, g_stop, f_start, f_stop;
	int ops_cnt;

	if (argc != num_args+1)
	{
		usage();
		return -1;
	}

	g_create_bias = atoi(argv[1]);
	g_rw_bias = atoi(argv[2]);
	g_append_bias = atoi(argv[3]);
	g_min_iosize = atoi(argv[4]);
	g_max_iosize = atoi(argv[5]);
	g_sleep = atoi(argv[6]);
	g_burst = atoi(argv[7]);
	g_fsync_freq = atoi(argv[8]);
	g_num_files = atoi(argv[9]);
	g_num_dirs = atoi(argv[10]);
	g_min_filesize = atoi(argv[11]);
	g_max_filesize = atoi(argv[12]);
	g_num_ops = atoi(argv[13]);
	g_min_time = atoi(argv[14]);
	g_seed = atoi(argv[15]);
	g_target_dir = argv[16];
	g_num_threads = atoi(argv[17]);

	if (!validate_params())
	{
		return -1;
	}

	print_test_params();

	srand(g_seed);

	/*init*/
	next_fileno_array = (int *)malloc(sizeof(int) * g_num_dirs);
	memset(next_fileno_array, 0, sizeof(int) * g_num_dirs);
	io_buf = (char *)malloc(sizeof(char) * g_max_iosize);
	fill_buf = (char *)malloc(sizeof(char) * g_max_filesize);
	burst_latency = (long *)malloc(sizeof(long) * g_burst);
	g_files_created = 0;
	g_files_deleted = 0;
	g_files_written = 0;
	g_files_read = 0;
	g_bytes_written = 0;
	g_bytes_read = 0;

	clock_gettime(CLOCK_MONOTONIC, &g_start);
	/*create dirs*/
	if (!create_dirs())
	{
		printf("directory creation failed!\n");
		cleanup();
		return -1;
	}

	/*create files for each dir*/
	for(i = 0; i < g_num_dirs; i++)
	{
		if (!create_files(i))
		{
			printf("file creation failed!\n");
			cleanup();
			return -1;
		}
		printf("filled dir %d\n", i);
	}
	sync();
	/*create threads*/

	/* print set up stats and reset global counters */
	//TODO
	clock_gettime(CLOCK_MONOTONIC, &g_stop);
	print_stats(diff_time(g_start, g_stop));
	g_files_created = 0;
	g_files_deleted = 0;
	g_files_written = 0;
	g_files_read = 0;
	g_bytes_written = 0;
	g_bytes_read = 0;

	
	/*run ops*/
	ops_cnt = 0;
	clock_gettime(CLOCK_MONOTONIC, &g_start);
	clock_gettime(CLOCK_MONOTONIC, &g_stop);
	clock_gettime(CLOCK_MONOTONIC, &f_start);
	while (ops_cnt < g_num_ops || (g_stop.tv_sec - g_start.tv_sec) < g_min_time)
	{
//		printf("elapsed: %ld\n", diff_time(g_start, g_stop));
		do_work();
		ops_cnt++;
		if (g_sleep > 0 && ops_cnt > 0 && (ops_cnt%g_burst) == 0)
		{
			clock_gettime(CLOCK_MONOTONIC, &start);
			sleep(g_sleep);
			clock_gettime(CLOCK_MONOTONIC, &stop);
			emit_time(S, diff_time(start, stop));
		}
		clock_gettime(CLOCK_MONOTONIC, &f_stop);
		if (diff_time(f_start, f_stop) >= g_fsync_freq*NANO_TO_S)
		{
			sync();
			emit_time(F, diff_time(f_start, f_stop));
			clock_gettime(CLOCK_MONOTONIC, &f_start);

		}
		clock_gettime(CLOCK_MONOTONIC, &g_stop);
	}

	print_stats(diff_time(g_start, g_stop));
	cleanup();
	return 0;
}
