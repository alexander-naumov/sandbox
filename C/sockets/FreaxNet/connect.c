/*
 *  This code is licenced under the GPL 2
 *		connect.c
 */ 


#include <error.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

int cls;
char nodename[200], servname[200];

char *get_proto()
{
	struct servent *s;
	s = getservent();
	return s->s_proto;
}

void fatal(char *msg)
{
	perror(msg);
	exit(1);
}

int connect_(char *host, int portnum)
{		
	int sock;
	struct sockaddr_in servadd;
	struct hostent *hp;
	
	if ((sock = socket(AF_INET, SOCK_STREAM, 0)) == -1)
		fatal("socket");

	bzero(&servadd, sizeof(servadd));

	if ((hp = gethostbyname(host)) == NULL)
		fatal("gethostbyname");
	
	servadd.sin_port = htons(portnum);
	servadd.sin_family = AF_INET;
	bcopy(hp->h_addr, (struct sockaddr *)&servadd.sin_addr, hp->h_length);

	int cr = connect(sock, (struct sockaddr *)&servadd, sizeof(servadd));
	if (cr != 0) {
		if(errno != ECONNREFUSED)
			fatal("connect");
		cls++; 
		cr = cls;
	}
	else
		cr = 0;
	
	getnameinfo((struct sockaddr *)&servadd, sizeof(servadd),
		nodename, sizeof(nodename), servname, sizeof(servname), 0);
	
	close(sock);
	return cr;
}
 
