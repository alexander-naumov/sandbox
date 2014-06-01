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
        int in_fd, out_fd, n_chars, result;
        char buf[4096];

        if ((in_fd = open("/home/transact/bin/list_of_ips.txt", O_RDONLY)) == -1) {
                perror("Cannot open file list_of_ips.txt");
                exit (1);
        }

        if ((out_fd = open("/home/opendkim/etc/opendkim/TrustedHosts", O_WRONLY)) == -1){
                perror("Cannot open file /home/transact/etc/opendkim/TrustedHosts");
                exit (1);
        }

	
	char *localhost = "127.0.0.1\n";
	char *str_size;
	write(out_fd, localhost, strlen(localhost));

        while((n_chars = read(in_fd, buf, 4096)) > 0) {
                if(write(out_fd, buf, n_chars) != n_chars) {
                        perror("Write error (to /home/transact/etc/opendkim/TrustedHosts)");
                        exit(1);
                }
        }
        
	if(n_chars == -1)
                printf("Read error (from list_of_ips.txt)");

        if(close(in_fd == -1) || close (out_fd) == -1) {
                perror("Error closing files");
                exit(1);
        }

//        if ((result = unlink("/home/transact/bin/list_of_ips.txt")) == -1) {
//                perror("Cannot delete file list_of_ips.txt");
//                exit(1);
//        }

        // #service opendkim restart    
        int restart;
        setuid (0);
        setgid (0);
        if ((restart = execl("/sbin/service", "service", "opendkim", "restart", NULL)) == -1) {
        	perror("Cannot restart opendkim module");
        	exit(1);
        }
	
	return 0;
}


