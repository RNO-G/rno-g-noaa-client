#define _XOPEN_SOURCE
#include <time.h> 

#include <stdio.h> 
#include <libpq-fe.h> 
#include <sys/socket.h> 
#include <stdlib.h> 
#include <string.h> 
#include <sys/types.h> 
#include <unistd.h>
#include <signal.h> 
#include <arpa/inet.h> 
#include <netinet/in.h> 

static struct 
{
  int udp_port; 
  const char * psql_conn_info; 
  int verbose; 
  int test; 
} opts = 
{ 
  .udp_port = 2101, 
  .psql_conn_info = "dbname=rno_g_met_live", 
  .verbose = 0, 
  .test = 0 
}; 

struct weather_packet 
{
  struct tm when;  
  struct tm recvd;  
  float wind_spd; 
  float wind_dir; 
  float gust_spd; 
  float pressure; 
  float dewpoint; 
  float temperature; 
}; 

int parse_args(int nargs, char ** args) 
{
  for (int iarg = 1; iarg < nargs; iarg++) 
  {
    if (!strcmp("-v",args[iarg]))
    {
        opts.verbose =1;
    }
    if (!strcmp("-t",args[iarg]))
    {
        opts.test =1;
    }
 
    else if (!strcmp("-p",args[iarg++]))
    {
      int port = 0; 
      if (iarg == nargs) 
        return 1; 
      port = atoi (args[iarg]); 
      if (!port) 
        return 1; 
      opts.udp_port = port; 
    }
    else if (!strcmp("-c", args[iarg++]))
    {
      if (iarg == nargs) 
        return 1; 
      opts.psql_conn_info = args[iarg]; 
    }
    else if (!strcmp("-h",args[iarg]))
    {
      return 1; 
    }
  }

  return 0; 
}

void usage() 
{
  printf("rno-g-noaa-client [-p port] [-c psql_conn_info] [-v] [-h] [-t]\n"); 
  printf("  -h               print this message\n"); 
  printf("  -v               enable verbose mode\n"); 
  printf("  -t               enable test mode (no db connection)\n"); 
  printf("  -p port=2101     the port to listen on\n"); 
  printf("  -c psql_conn_info=dbname=rno_g_met_live the options to pass to pgsql\n"); 
}

PGconn * db = NULL; 
const char * insert_stmt_name = "noaa-client"; 
const char * insert_stmt = 
  "INSERT INTO obs (obs_time, insert_time, wind_speed, wind_dir, gust_speed, pressure, temperature, dewpoint)"
  "VALUES ($1::integer, $2::integer, $3::real, $4::real, $5::real, $6::real, $7::real, $8::real)";  

int sock = 0; 
char buf[256]; 

volatile int quit = 0; 

void insert(struct weather_packet  *w) 
{
  const char * dbvals[8]; 
  int lens[8] = {0,0,4,4,4,4,4,4}; 
  int bin[8] = {0,0,1,1,1,1,1,1}; 


  char whenbuf[24]; 
  strftime(whenbuf, sizeof(whenbuf), "%Y-%m-%d %H:%M:%SZ", &w->when);
  char recvbuf[24]; 
  strftime(recvbuf, sizeof(recvbuf), "%Y-%m-%d %H:%M:%SZ", &w->recvd);

  union { float f; uint32_t u; char c[4];}  be[6]; //big endian, since using binary format 
  be[0].f = w->wind_spd; 
  be[1].f = w->wind_dir; 
  be[2].f = w->gust_spd; 
  be[3].f = w->pressure; 
  be[4].f = w->temperature; 
  be[5].f = w->dewpoint; 

  for (int i = 0; i < 6; i++) be[i].u = htonl(be[i].u); 

  dbvals[0] = whenbuf; 
  dbvals[1] = recvbuf; 
  dbvals[2] = be[0].c; 
  dbvals[3] = be[1].c; 
  dbvals[4] = be[2].c; 
  dbvals[5] = be[3].c; 
  dbvals[6] = be[4].c;  
  dbvals[7] = be[5].c; 

  int nattempts = 0;
  do 
  {
    PGresult * res = PQexecPrepared(db, insert_stmt_name, 8, dbvals, lens, bin, 0); 

    if (PQresultStatus(res) != PGRES_COMMAND_OK) 
    {
       fprintf(stderr, "exec failed %d: %s\n   %s\n", PQresultStatus(res), PQresultErrorMessage(res),PQerrorMessage(db)); 
       PQreset(db); //try to reconnect 
    }
    PQclear(res); 
  } while ( nattempts < 3); 

}

void sighandler(int signum) 
{
  quit = signum; 
}

void cleanup() 
{
  if (db) PQfinish(db); 
  if (sock>0) close(sock); 
}

void fail() 
{
  cleanup(); 
  exit(1); 
}

int main(int nargs, char ** args) 
{

  int ok = parse_args(nargs, args); 
  if (ok) 
  {
    usage(); 
    return ok; 
  }
  
  if (!opts.test) 
  {
    db = PQconnectdb(opts.psql_conn_info); 
    if (!db) 
    {
      fprintf(stderr,"Failed to connect to db with conninfo=%s\n", opts.psql_conn_info); 
      return 1; 
    }
    PGresult * res = PQprepare(db,  insert_stmt_name, insert_stmt,0,0); 
    if (PQresultStatus(res) != PGRES_COMMAND_OK) 
    {
      fprintf(stderr,  "PREPARE failed %d: %s\n%s\n",PQresultStatus(res),PQresultErrorMessage(res), PQerrorMessage(db)); 
      fail(); 
    }
    PQclear(res); 
  }

  //open the socket 

  sock = socket(AF_INET, SOCK_DGRAM, 0); 
  if (sock < 0) 
  {
    fprintf(stderr,"Could not create socket\n"); 
    fail(); 
  }

  struct sockaddr_in addr = {.sin_family = AF_INET, .sin_addr = {.s_addr = INADDR_ANY}, .sin_port = htons(opts.udp_port) }; 
  //bind tothe port 
  if (bind(sock,(const struct sockaddr*)  &addr, sizeof(addr)) < 0)
  {
    fprintf(stderr,"Failed to bind to port %d\n", opts.udp_port); 
    fail(); 
  }

  signal(SIGINT, sighandler); 
  signal(SIGQUIT, sighandler); 

  while (!quit) 
  {

    socklen_t len = sizeof(addr); 
    int n = recvfrom(sock, buf, sizeof(buf), 0, (struct sockaddr*) &addr, &len); 
    if (quit) break; 
    if (!n) continue; //non-fatal signal? 
    time_t now = time(NULL); 

    buf[n] = 0; //make sure string is zero terminated 
    if (opts.verbose) 
    {
      printf("From [%s:%d]:: %s\n", inet_ntoa(addr.sin_addr ), ntohs(addr.sin_port),  buf); 
    }
             

    //set up the weather packet
    struct weather_packet w= {.recvd = *gmtime(&now), .wind_spd = -999, .wind_dir = -999, .gust_spd = -999, .pressure = -999, .temperature = -999, .dewpoint = -999}; 

    char * comma = strchr(buf,','); 
    *comma = 0; 
    strptime(buf, "\"%Y-%m-%d %H:%M:%S\"", &w.when); 

    sscanf(comma+1, "%f,%f,%f,%f,%f,%f", 
                  &w.wind_spd, &w.wind_dir, &w.gust_spd,
                  &w.pressure, &w.temperature, &w.dewpoint); 


    if (!opts.test) insert(&w); 
  }

  fprintf(stderr,"Got signal %d\n", quit); 

  cleanup(); 
  return 0; 
}
