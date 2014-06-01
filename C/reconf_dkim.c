/*	-*- mode: c; mode: fold -*-   	*/

# include	<stdlib.h>
# include	<stdio.h>
# include	<unistd.h>
# include	<sys/stat.h>
# include	<sys/types.h>
# include	<string.h>
# include	<errno.h>
# include	<fcntl.h>
# include	<pwd.h>
# include	<grp.h>

# define	eof	"\n"

void
key_table_add(char *domain, char *selector)
{
	//echo "(s)._domainkey.(d) (d):(s):/etc/opendkim/keys/(d)/(s)" >> /etc/opendkim/KeyTable	
	int fd, result;
        char *str_key_table;

	char *str_kt_0 = "._domainkey.";
	char *str_kt_1 = " ";
	char *str_kt_2 = ":/home/opendkim/etc/opendkim/keys/";
	char *str_kt_3 = "/";
	char *str_kt_4 = ":";

	str_key_table = malloc(
				strlen(selector) +
				strlen(str_kt_0) +
				strlen(domain) +
				strlen(str_kt_1) +
				strlen(domain) +
				strlen(str_kt_4) +
				strlen(selector) +
				strlen(str_kt_2) +
				strlen(domain) +
				strlen(str_kt_3) +
				strlen(selector) + 1);

	if (str_key_table) {
		strcpy(str_key_table, selector);
		strcat(str_key_table, str_kt_0);
		strcat(str_key_table, domain);
                strcat(str_key_table, str_kt_1);
                strcat(str_key_table, domain);
		strcat(str_key_table, str_kt_4);
		strcat(str_key_table, selector);
                strcat(str_key_table, str_kt_2);
                strcat(str_key_table, domain);
                strcat(str_key_table, str_kt_3);
		strcat(str_key_table, selector);
		strcat(str_key_table, eof);
	}
	
	if ((fd = open("/home/opendkim/etc/opendkim/KeyTable", O_WRONLY | O_APPEND)) == -1){
		perror("Cannot open file /home/opendkim/etc/opendkim/KeyTable");
		exit (1);
	}
	
	write(fd, str_key_table, strlen(str_key_table));

	if ((result = close(fd)) == -1){
		perror("Cannot close file /home/opendkim/etc/opendkim/KeyTable");
		exit(1);
	}
	return;
}

void
signing_table_add(char *domain, char *selector)
{
        //echo "*@(d) (s)._domainkey.(d)" >> /etc/opendkim/SigningTable
	char *str_signing_table;
	char *str_st_0 = "*@";
	char *str_st_1 = "._domainkey.";
	char *str_st_2 = " ";
	int fd, result;	

	str_signing_table = malloc(strlen(str_st_0) + strlen(domain) + strlen(str_st_2) + strlen(selector) + strlen(str_st_1) + strlen(domain) + 1);
	if (str_signing_table) {
		strcpy(str_signing_table, str_st_0);
                strcat(str_signing_table, domain);
                strcat(str_signing_table, str_st_2);
		strcat(str_signing_table, selector);
		strcat(str_signing_table, str_st_1);
                strcat(str_signing_table, domain);
		strcat(str_signing_table, eof);
	}

        if ((fd = open("/home/opendkim/etc/opendkim/SigningTable", O_WRONLY | O_APPEND)) == -1){
                perror("Cannot open file /home/opendkim/etc/opendkim/SigningTable");
                exit (1);
        }

        write(fd, str_signing_table, strlen(str_signing_table));

        if ((result = close(fd)) == -1){
                perror("Cannot close file /home/opendkim/etc/opendkim/SigingTable");
                exit(1);
        }

        return;
}

void
keys(char *PATH, char *key, char *selector, uid_t uid, gid_t gid)
{
	char *default_ = "/";
	char *str_path;
	int fd, result;

	str_path = malloc(strlen(PATH) + strlen(default_) + strlen(selector) + 1);
        if (str_path) {
                strcpy(str_path, PATH);
		strcat(str_path, default_);
                strcat(str_path, selector);
        }

        if ((fd = open(str_path, O_WRONLY | O_CREAT | O_TRUNC, 0600)) == -1) {
                perror("Cannot open file (private key)");
                exit (1);
        }

        write(fd, key, strlen(key));

        if ((result = close(fd)) == -1){
                perror("Cannot close file (private key)");
                exit(1);
        }

        if ((fd = chown(str_path, uid, gid)) == -1) {
                perror("Cannot change file's owner/group (private key)");
                exit(1);
        }       
}

int
main (int argc, char **argv)
{
	if ((argc <= 1) || ((argc == 2) && (! strcasecmp (argv[1], "help")))) {
		fprintf (stderr, "Usage: %s <domain> <privat_key> <selector>\n", argv[0]);
		return 0;
	}

	// mkdir /etc/opendkim/keys/(i)
	char *s1 = "/etc/opendkim/keys/";
	char *s2 = argv[1];
	char *key = argv[2];
	char *selector = argv[3];
	char *PATH;

	PATH = malloc(strlen(s1) + strlen(s2) + 1);
	if (PATH) {
		strcpy(PATH, s1);
		strcat(PATH, s2);
	}

	int fd;
	if ((fd = mkdir(PATH, S_IRWXU)) == -1) {
		perror("Cannot create directory");
		exit(1);
	}

	//chown -R opendkim:opendkim /etc/opendkim/keys/(i)
	struct passwd	*pwd;
	struct group	*grp;
	uid_t		uid;
	gid_t		gid;
	
	setpwent ();
	if (! (pwd = getpwnam ("opendkim"))) {
		perror ("No user opendkim");
		exit (1);
	}
	uid = pwd -> pw_uid;
	endpwent ();
	setgrent ();
	if (! (grp = getgrnam ("opendkim"))) {
		perror ("No group opendkim");
		exit (1);
	}
	gid = grp -> gr_gid;
	endgrent ();
		
	if ((fd = chown(PATH, uid, gid)) == -1) {
		perror("Cannot change directory's owner/group");
		exit(1);
	}

        key_table_add(s2, selector);
        signing_table_add(s2, selector);
	keys(PATH, key, selector, uid, gid);

	// #service opendkim restart
        int restart, s_uid, s_gid;
        if ((s_uid = setuid (0)) < 0) {
                perror("setuid");
                exit(1);
        }

        if ((s_gid = setgid (0)) < 0) {
                perror("setgid");
                exit(1);
        }
	
	if ((restart = execl("/sbin/service", "service", "opendkim", "restart", NULL)) == -1) {
		perror("Cannot restart opendkim module");
		exit(1);
	}
	

        return 0;
}

