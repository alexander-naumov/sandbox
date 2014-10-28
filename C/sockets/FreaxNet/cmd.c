/*
 *  This code is licenced under the GPL 2.
 *		cmd.c
 */


#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>

#include "cmd.h"
#define VERSION_STRING "ver 0.062"


char IP[IP_LENGTH];
int begin_port = 1;
int end_port = 3000;
int rvn = 0;

int get_num(char **ps) 
{
	int x = 0;
	char *s = *ps;
	if(*s < '0' || *s > '9')
		return -1;
	do{
		x *= 10;
		x += *s - '0';
		s++;
	} while(*s >= '0' && *s <= '9');
	*ps = s;
	return x;
}

void run_error()
{
	printf("FreaxNet v0.062\n");
	printf("USAGE:\t  FreaxNet [Options] <IP_address>[:port]|[:begin_port-end_port]\n");
	printf("OPTIONS:  -h <host>\n\t  -f <file>\n\t  -p <pause>\n");
	printf("EXAMPLES: FreaxNet -p 2 127.0.0.1\n\t  FreaxNet -h 127.0.0.1:16-256\n\t  FreaxNet -f hosts");
	printf("\nSee the man page for more options and examples %)\n\n");
}

int cmd(char *buf)
{
	if (strlen(buf) >= IP_LENGTH-1) {
		fprintf(stderr, "It is a very long address: %s\n", buf);
		return 1;
	}
	strcpy(IP, buf);	
	char *p = strrchr(IP, ':');
	if (p) {
		*p = '\0';
		p++;
		begin_port = get_num(&p);
		if(begin_port >= 0 && *p == '-') {
			p++;
			end_port = get_num(&p);
		}
		else {
			end_port = begin_port;
			rvn = 1;
		}
		if(begin_port < 0 || end_port < 0) {
			fprintf(stderr, "Invalid port number \"%s\"\n", p);
			return 1;
		}
	}
	return 0;
}
 
