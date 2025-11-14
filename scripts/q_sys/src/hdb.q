include: {
  curFile: value[{}][6];
  toLoad: sublist[1+last where curFile = "/"; curFile], x;
  -1 "loading ", toLoad;
  system "l ", toLoad;
  };

include "common.q";
initLog[`hdb];

system "c 25 300";

.qlog.info "Starting benchmarking HDB process";
.qlog.info "kdb+ version: ", string[.z.K], ", kdb+ minor version: ", string[.z.k], ", PID: ", string[.z.i], 
  " port: ", string[system "p"], " thread count: ", string system "s";

DBDIR: cmd `db;
system "mkdir -p ", DBDIR;  // DB might be empty, ready for population
NEEDQMAP: "true" ~ lower getenv `QMAP;
QUERYRETRY: `$lower getenv `QUERYRETRY;

include cmd `query;  // loading queries

//Simple helper function to reload the HDB to see newly saved data
reloadHDB:{
    .qlog.info "(re)loading data from ", DBDIR;
    system "l ", DBDIR;
    if[NEEDQMAP;
      .qlog.info "Calling .Q.MAP[]";
      .Q.MAP[]];
 };

if[(not `par.txt in key hsym `$DBDIR) or `sym in key hsym `$DBDIR; reloadHDB[]];

if[ `writer in key cmd;
  CONNTIMEOUTINSEC: 0D00:05;  // We wait maximum 5 minutes for the writer
  WRITERPORT: "J"$cmd `writer;
  //Open a handle to the writer
  WriterconnStart: .z.P;
  while[(WriterconnStart > .z.P - CONNTIMEOUTINSEC) & not writerHandle:@[hopen; WRITERPORT; {[err] .qlog.info "writer is not available (yet) ", err; 0 }];
      system "sleep 1;"];
  if[not writerHandle;
      .qlog.error "Could not connect to writer after several retry. Quitting ...";
      exit 1;
   ];
  .qlog.info "Connection to writer established successfully";
  writerHandle "subscribe[]";

  .z.pc:{if[x = writerHandle; .qlog.warn "Lost connection to writer!"]}
  ];

retryQueryIfStop: {[q;e] 
  .qlog.error "Error executing query: ", q, " . Error: ", e;
  if[not "stop" ~ e; 'e];
  
  .qlog.info "Doing garbage collection and retrying query";
  .Q.gc[0];
  @[value; q; {[e2] .qlog.error "Error executing query the second time: ", e2; 'e2}]
  };

logQueryError: {[q;e] 
  .qlog.error "Error executing query: ", q, " . Error: ", e; 
  'e
  };

errorHandler: $[QUERYRETRY ~ `retry;retryQueryIfStop;logQueryError]

runQuery: {[q]
//    .d.q:q;
    @[value; q; errorHandler[q]]
  };

/
.z.pg:{.qlog.debug "Sync request: ", .Q.s1 x; value x};
.z.ps:{.qlog.debug "Async request: ", .Q.s1 x; value x};

.z.po: {
    .qlog.debug "Input connection established on handle ", string x;
 };

\