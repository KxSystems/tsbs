include "common.q";
initLog[`writer];

.qlog.info "Starting benchmarking writer process";
.qlog.info "kdb+ version: ", string[.z.K], ", kdb+ minor version: ", string[.z.k], ", PID: ", string[.z.i], " port: ", string system "p";

DBDIR: cmd `db;
DBDIRFH: hsym `$DBDIR;
HASPARTXT: `par.txt in key DBDIRFH

getPathFn: $[HASPARTXT; [
  DBDIRFHS: hsym `$read0 .Q.dd[DBDIRFH; `par.txt];
  {[dt] .Q.par[DBDIRFHS dt mod count DBDIRFHS; dt]}
  ];
  {[dt] .Q.par[DBDIRFH; dt]}]

include cmd `schema;  // loading schema

// @desc Appends columns in parallel.
// @param path	{symbol}	Target path.
// @param tab	{table}		Data to append.
// @return	{symbol}	Target path.
parallelUpsert:{[path;tab]
    if[not count key path; .Q.dd[path;`.d] set cols tab];
	{[path;tab;c] .[.Q.dd[path;c];();,; tab c]}[path;tab]peach cols tab;
	tab
	}

//
// @desc Enumerates symbol columns in a table, updating the global symbol table as required.
//
// This function is a faster version of `.Q.en` that does not read and materialize the
// symbol table from disk before updating it.  The current symbol table must already be
// defined in memory.  The on-disk symbol table is rewritten only when new symbols are
// enumerated.
//
// @param d {symbol}	Specifies the directory where the symbol file is located.
// @param t {table}		Specifies the table whose symbol columns are to be enumerated.
//
// @return {table}		The incoming table, with symbol columns enumerated.
//
k)en:{[x;d;t;s] if[#c@:&{$[11h=@*x;&/11h=@:'x;11h=@x]}'t c:!+t;n:#. s;s??,/?:'{$[0h=@x;,/x;x]}'t c;if[n<#. s;.[`/:d,s;();,;n_. s]]];@[t;c;{$[0h=@z;(-1_+\0,#:'z)_x[y;,/z];x[y;z]]}[x;s]]}[?;;;`sym]

NATIVEQEN: (0= count getenv `QEN) | "native" ~ getenv `QEN;
QEN: $[NATIVEQEN; .Q.en;
  [.qlog.info "custom .Q.en will be used"; `sym set (); en]];

upsertFN: $[(0= count getenv `UPSERT) | "native" ~ getenv `UPSERT;
    {.Q.dd[x;`] upsert y};  // add trailing / for splaying
    [.qlog.info "parallel upsert will be used"; parallelUpsert]];

updS: {[d;r]
    // .debug.upd: (d;r);        // useful for debugging
    // `d`r set' .debug.upd      // do manually during debug

    upd[`readings] flip schema[`readings][`c]!(schema[`readings][`t]; csv) 0: r;
    upd[`diagnostics] flip schema[`diagnostics][`c]!(schema[`diagnostics][`t]; csv) 0: d;


    postUpd[];
    :1b
  };

//upd expects the data to come in as two lists of strings(diagnostics and readings), where each string is a CSV row
persist: {[tName; t]
    //Save the data for each date
    t: (sortColsDisk, prtnCol) xcols QEN[DBDIRFH] t;
    upsertToDatePartition[;t; tName] each exec distinct `date$timestamp from t;
 };

include "writerApi.q";
indexOnFleet: "true" ~ lower getenv `INDEXONFLEET;

sortAndMakeParted: {[path]
  (sortColsDisk, prtnCol) xasc path;
  @[path; sortColsDisk; `p#];
  if[indexOnFleet; @[path; `fleet; `g#]];
  };

subscribers: ();
subscribe: {[]
  .qlog.info "new subscription: ", string .z.w;
  subscribers,: .z.w};
.z.pc: {if[x in subscribers;
  .qlog.info "removing subscriber ", string x;
  `subscribers set subscribers except x]};