# include       <stdlib.h>
# include       <stdio.h>
# include       <unistd.h>
# include       <sys/stat.h>
# include       <sys/types.h>
# include       <string.h>
# include       <errno.h>
# include       <fcntl.h>
# include       <pwd.h>
# include       <grp.h>



int main()
{
	int out_fd, in_fd, n_chars;
	char buf[4096];

        if ((out_fd = open("aaa.txt",  O_WRONLY)) == -1) {
                perror("Cannot open file list_of_ips.txt");
                exit (1);
        }


        char *localhost = "127.0.0.1\n";
	write(out_fd, localhost, strlen(localhost));


        if ((in_fd = open("access", O_RDONLY)) == -1) {
                perror("Cannot open file list_of_ips.txt");
                exit (1);
        }

        while((n_chars = read(in_fd, buf, 4096)) > 0) {
                if(write(out_fd, buf, n_chars) != n_chars) {
                        perror("Write error (to /home/transact/etc/opendkim/TrustedHosts)");
                        exit(1);
                }
        }

        if(close(in_fd == -1) || close (out_fd) == -1) {
                perror("Error closing files");
                exit(1);
        }



	return 0;
}
