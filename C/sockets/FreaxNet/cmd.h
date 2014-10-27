/*
 * This code is licenced under the GPL 2
 *		cmd.h
 */
#define IP_LENGTH 128

extern char IP[];
extern int begin_port;
extern int end_port;
extern int rvn;
int cmd(char *buf);
