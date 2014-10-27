/*
 *  This code is licenced under the GPL 2.
 * 		main.c
 */


#include <stdio.h>
#include <unistd.h>

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include "cmd.h"
#include "connect.h"


int cls, paus;
char servname[200];
struct servent *s;

int scan(char *argv)
{
	int port;
	int r = 0;
	if(cmd(argv))
		return 1;
	
	cls = 0;
	printf("\nScanning for %s:%d-%d", IP, begin_port, end_port);
	
	if(paus != 0)
	    printf(" (with the pause = %d)", paus);
	    
	printf("\n\n    PORT  STATUS SERVICE\n");
	for (port = begin_port; port <= end_port; port++)
	{
		sleep(paus);
		r = connect_(IP, port);
		if(r < 0)
			return 1;
		if(r > 0)
		{
			if(rvn == 1)
				printf("     d%   close\n", begin_port);
			else
				printf("\b\b\b\b\b% 5d", cls);
		}
		else
			printf("\b\b\b\b\b% 5d\\%s  open   %s\n", port, get_proto(), servname);	
	}	
	if(rvn == 1)
		printf("\n");
	else
		printf(" port's is closed\n\n");

}

int parse_file(char *name)
{
	FILE *f=fopen(name,"r");
	char buf[128];
	if(f!=0)
	{
		while(fgets(buf,128,f)!=0)
		{
			scan(buf);
		}
		fclose(f);
	}
	else
		printf("Invalid file\n");
}

int main(int argc, char *argv[])
{
	if(argv[1] == NULL)
	    run_error();
	
	int c,r=0;//r=1 host:port ; r=2 - from file
	char *command;
	opterr=0;
	
	while((c=getopt(argc,argv,"h:f:p:"))!=-1)
	{
		switch(c)
		{
			//h-host = host:port, host:port-port
			case 'h':   	command=optarg;
					r=1;
					break;
				    
			case 'f':	command=optarg;
					r=2;
					break;
					
			case 'p':	command=argv[3];
					paus = atoi(optarg);
					r=1;
					break;
			
			default:
					run_error();
		}
	}
	if(r==1)
		scan(command);
	else if(r==2)
		parse_file(command);
	return 0;
}
