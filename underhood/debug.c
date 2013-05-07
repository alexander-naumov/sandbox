#include <stdio.h>
#include <sys/reg.h>
#include <sys/types.h>
#include <sys/ptrace.h>
#include <unistd.h>
#include <sys/wait.h>
#include <stdarg.h>
#include <stdlib.h>
#include <syscall.h>
#include <sys/user.h>
#include <unistd.h>
#include <errno.h>

void run_target(const char* programname)
{
	printf("target started. will run '%s'\n", programname);

	/* Allow tracing of this process */
	if (ptrace(PTRACE_TRACEME, 0, 0, 0) < 0) {
		perror("ptrace");
		return;
	}

	/* Replace this process's image with the given program */
	execl(programname, programname, 0);
}


void run_debugger(pid_t child_pid)
{
	int wait_status;
	unsigned icounter = 0;
	printf("debugger started\n");

	/* Wait for child to stop on its first instruction */
	wait(&wait_status);

	while (WIFSTOPPED(wait_status)) {
		icounter++;
		struct user_regs_struct regs;
		ptrace(PTRACE_GETREGS, child_pid, 0, &regs);
		unsigned instr = ptrace(PTRACE_PEEKTEXT, child_pid, regs.rip, 0);

		printf("icounter = %u.  RIP = 0x%08x.  instr = 0x%08x\n",
			icounter, regs.rip, instr);

	/* Make the child execute another instruction */
		if (ptrace(PTRACE_SINGLESTEP, child_pid, 0, 0) < 0) {
			perror("ptrace");
			return;
		}

	/* Wait for child to stop on its next instruction */
		wait(&wait_status);
	}

	printf("the child executed %u instructions\n", icounter);
}

int main(int argc, char** argv)
{
	pid_t child_pid;

	if (argc < 2) {
		fprintf(stderr, "Expected a program name as argument\n");
		return -1;
	}

	child_pid = fork();
	if (child_pid == 0)
		run_target(argv[1]);
	else if (child_pid > 0)
		run_debugger(child_pid);
	else {
		perror("fork");
		return -1;
	}
	return 0;
}
