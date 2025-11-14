include: {
  curFile: value[{}][6];
  toLoad: sublist[1+last where curFile = "/"; curFile], x;
  -1 "loading ", toLoad;
  system "l ", toLoad;
  };
include "writer_common.q";

WRITERCACHESIZE: 0^"J"$getenv `WRITERCACHESIZE;
.qlog.info "Cache size is set to ", string WRITERCACHESIZE;

createEmptyTable each key schema;

upd: $[WRITERCACHESIZE;
  {[tName; t]
   tName insert t;
   if[WRITERCACHESIZE < count value tName;
      persist[tName; value tName];
      delete from tName];
   }; persist];

upsertToDatePartition:{[dt; tab; tabName]
    // .debug.ups:(dt; tab; tabName);     // useful for debugging
    // `dt`tab`tabName set' .debug.ups;   // do manually during debug

    // Select data of the given date
    tab: ?[tab; enlist(=; (`date$; prtnCol); dt); 0b; ()];
    path: getPathFn[dt] tabName;
    upsertFN[path] tab;
 };

publisherNr: 0;
init: {
  .qlog.info "New publisher";

  publisherNr+: 1;
  :1b
  };

getDates: {[dbdir] "D"$string except[; `sym] key dbdir}
getAllTablePaths: {[dbdir] .Q.par[dbdir] .' getDates[dbdir] cross key schema}

finish: {
  .qlog.info "A publisher finished";
  publisherNr-: 1;

  if[ publisherNr = 0;
    .qlog.info "All publishers finished";
    .qlog.info "Persisting data from local cache";
    {[tName] persist[tName; value tName]; delete from tName} each key schema;
    if[ not NATIVEQEN;
      .qlog.info "persisting sym";
      .Q.dd[DBDIRFH; `sym] set sym];
    .qlog.info "Fill missing partitions";
    .Q.chk DBDIRFH;
    allDates: $[HASPARTXT; raze getDates each DBDIRFHS; getDates DBDIRFH];
    $[count allDates; [
      .qlog.info "Nr of partitions: ", string count allDates;
      .qlog.info "Sorting tables and adding attribute(s)";
      sortAndMakeParted each $[HASPARTXT; raze getAllTablePaths each DBDIRFHS; getAllTablePaths DBDIRFH]
      .qlog.info "Database created";
      .qlog.info "Notifying subscribers";
      neg[subscribers]@\:"reloadHDB[]"
      ];
      .qlog.error "No partitions created!"];
  ];
  :1b
  };
