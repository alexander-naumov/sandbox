//SCTPClient.C
 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/sctp.h>
#include <arpa/inet.h>
#define MAX_BUFFER 1024
 
void die(char *s)
{
  perror(s);
  exit(1);
}
 
 
int main()
{
  int connSock, in, i, ret, flags;
  struct sockaddr_in servaddr;
  struct sctp_status status;
  char buffer[MAX_BUFFER+1];
 
  connSock = socket( AF_INET, SOCK_STREAM, IPPROTO_SCTP );
 
  if(connSock == -1)
    die("socket()");
 
  bzero( (void *)&servaddr, sizeof(servaddr) );
  servaddr.sin_family = AF_INET;
  servaddr.sin_port = htons(9999);
  servaddr.sin_addr.s_addr = inet_addr( "127.0.0.1" );
 
  ret = connect( connSock, (struct sockaddr *)&servaddr, sizeof(servaddr) );
   
  if(ret == -1)
     die("connect()");
 
    printf("Enter data to send : ");
    scanf("%s",buffer);
    //getchar();
     
    ret = sctp_sendmsg( connSock, (void *)buffer, (size_t)strlen(buffer),
                       NULL, 0, 0, 0, 0, 0, 0 );
    close(connSock);
 
  return 0;
}

