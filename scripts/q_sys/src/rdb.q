include: {
  curFile: value[{}][6];
  toLoad: sublist[1+last where curFile = "/"; curFile], x;
  -1 "loading ", toLoad;
  system "l ", toLoad;
  };

include "rdb_common.q";

upd: insert;

publisherNr: 0;
init: {
  .qlog.info "New publisher";

  publisherNr+: 1;
  :1b
  };

finish: {
  .qlog.info "A publisher finished";
  publisherNr-: 1;

  if[ publisherNr = 0;
    .qlog.info "All publishers finished";
    .qlog.info "Sorting tables and adding indices";
    sortAndAddAttr each key schema;
    .qlog.info "Database created"];

   :1b
  };