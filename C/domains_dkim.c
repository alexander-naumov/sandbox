/*      -*- mode: c; mode: fold -*-     */

# include	<stdio.h>
# include	<sys/types.h>
# include	<dirent.h>	
# include       <string.h>

int main()
{
	DIR *dir_ptr;
	struct dirent *direntp;

	if ((dir_ptr = opendir("/home/opendkim/etc/opendkim/keys")) == NULL)
		fprintf(stderr, "Cannot open /etc/opendkim/keys\n");
	else
	{
		while((direntp = readdir(dir_ptr)) != NULL)
			if (direntp->d_name[0] != '.' && direntp->d_name[strlen(direntp->d_name)-1] != '~')
				printf("%s\n", direntp->d_name);
		closedir(dir_ptr);
	}

	return 0;
}
