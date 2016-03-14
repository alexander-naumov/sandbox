/*
 *	This is backend for VPN SSL check.
 *
 *	This program uses 'openconnect' to be able to establish the VPN
 *	connection using login and password. 
 *
 *	The 'openconnect' provides a client for Cisco's "AnyConnect" VPN,
 *	which uses HTTPS and DTLS protocols.  AnyConnect is supported
 *	by the ASA5500 Series, by IOS 12.4(9)T or later on Cisco SR500,
 *	870, 880, 1800, 2800, 3800, 7200 Series and Cisco 7301 Routers,
 *	and probably others.
 *
 *	Alexander Naumov <alexander_naumov@opensuse.org>, 2015
 *
 *	INSTALL:
 *			# gcc -Wall vpn_ssl_check.c -o vpn_ssl_check
 *			# chown root:centreon vpn_ssl_check
 *			# chmod 4454 vpn_ssl_check
 */

#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>

#define IFACE "tun8"

void usage()
{
  printf("Usage: ./vpn_ssl_check <URL_ADDRESS> <LOGIN> <PASSWORD>\n\n");
  exit(2);
}

void sigint()
{
	signal(SIGINT, sigint);
	exit(0);
}

int conn(char *cmd)
{
  int check, status = 0;
	setuid(0);
  pid_t pid = fork();

  if (pid == 0) {
		signal(SIGINT,sigint);
		if ((status = system(cmd)) == -1) {
	      perror("Cannot create VPN connection...");
  	    exit(1);
    }
  }
	else {
		sleep(10);
		check = parse_output();
		system("pkill -INT openconnect > /dev/null");
		printf("CHECK_STATE %d\n", check);
	}
	return check;
}

int parse_output()
{
	FILE *fp;
  char *line = NULL;
  size_t len = 0;
  ssize_t read;
  char *c;

  fp = fopen("/proc/net/dev", "r");
  if (fp == NULL)
    exit(EXIT_FAILURE);

  while ((read = getline(&line, &len, fp)) != -1) {
    c = strstr(line, IFACE);
    if (c) 
      return 0;
  }
	return 2;
}


int main(int argc, char* argv[])
{
  if (argc != 4)
    usage();
	
	char *url = argv[1];
	//printf ("url = %s\n", url);
	char *login = argv[2];
	//printf ("login = %s\n", login);
	char *pass = argv[3];
	//printf ("pass = %s\n", pass);	

	char *echo = "/bin/echo '";
	char *pipe = "' | ";
	char *space = " ";
	char *connect = "/usr/sbin/openconnect";
	char *options = " -i tun8 --passwd-on-stdin -u ";

	char *cmd;
	cmd = malloc(
			strlen(echo) +
			strlen(pass) +
			strlen(pipe) +
			strlen(connect) +
			strlen(space) +
			strlen(url) +
			strlen(options) +
			strlen(login) + 1);

	if (cmd) {
		strcpy (cmd, echo);
		strcat (cmd, pass);
		strcat (cmd, pipe);
		strcat (cmd, connect);
		strcat (cmd, space);
		strcat (cmd, url);
		strcat (cmd, options);
		strcat (cmd, login);
	}
	return conn(cmd); 
}
