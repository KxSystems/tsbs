include: {
  curFile: value[{}][6];
  toLoad: sublist[1+last where curFile = "/"; curFile], x;
  -1 "loading ", toLoad;
  system "l ", toLoad;
  };

include "common.q";

initLog[`$"bridge", string .z.i];

port: first .z.x;

include last .z.x;
include "writerApi.q";


writerS: $["true" ~ lower getenv `UNIXSOCKET; "unix://"; "localhost:"], port;
.qlog.info "connecting to  ", writerS;
writer: hopen hsym `$writerS;
writerDuringIngest: $["sync" ~ lower getenv `TRANSFERTOWRITER; writer; neg writer];


upd: {[tName; t]
  writerDuringIngest (`upd; tName; t);
  };

init: {
  writerDuringIngest (`init; ::);
  :1b
  };

finish: {
  writer (`finish; ::);
  :1b
  };