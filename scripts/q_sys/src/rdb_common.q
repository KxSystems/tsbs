include "common.q";
initLog[`rdb];

.qlog.info "Starting benchmarking RDB process";
.qlog.info "kdb+ version: ", string[.z.K], ", kdb+ minor version: ", string[.z.k], ", PID: ", string[.z.i], " port: ", string system "p";

include first .z.x;  // loading schema
include last .z.x;   // loading queries

createEmptyTable each key schema;

include "srcPrefixFormat.q";
indexOnFleet: "true" ~ lower getenv `INDEXONFLEET;

sortAndAddAttr: {[tName]
  prtnCol xasc tName;
  ![tName; (); 0b; enlist[sortColsDisk]!enlist (`g#; sortColsDisk)];
  if[indexOnFleet; update `g#fleet from tName];
 };