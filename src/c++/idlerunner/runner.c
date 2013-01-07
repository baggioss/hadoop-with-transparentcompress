#include <sched.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

#define SCHED_IDLE 5

static void printUsage(const char* cmd)
{
   printf("Usage: %s <cmd> [arg 1] [arg2] ... [arg9]\n", cmd);
}

int main(int argc, char* argv[])
{
  extern char **environ;
  struct sched_param param;
  param.sched_priority = 0;
  if (argc >= 10)
  {
    printUsage(argv[0]);
    return -1;
  }
  sched_setscheduler(0, SCHED_IDLE, &param);
  if (-1 == execve("/bin/sh", argv, environ))
  {
    printf("exec error. errno is %d. errstr is %s\n", errno, strerror(errno));
  }
}
