updSrcPrefixedStringsSingleTable: {[rows; tName]
  rowsSingleTable: rows where rows like string[tName], csv, "*";
  parsed: ("S",schema[tName][`t]; csv) 0: rowsSingleTable;
  upd[tName] delete tabName from flip (`tabName, schema[tName][`c])!parsed;
 };

EPOCHOFFSET: `long$1970.01.01D00 - 2000.01.01D00;

// This is not a performance optimized parser
updInfluxSingleTable: {[rows; tName]
  rowsSingleTable: rows where rows like string[tName], csv, "*";
  triples: " " vs' rowsSingleTable;
  types: schema[tName][`c]!schema[tName][`t];
  upd[tName] ([] timestamp: `timestamp$EPOCHOFFSET + "J"$triples[; 2]) ,'
    flip types[key m]$m: flip types {[types; r] @[; where types="I"; _[-1]] 1_(!/) "S=," 0: r}/: "tbls=",/: csv sv' triples[;0 1];
  };



updQBinarySingleTable: {[rows; tName]
  parsed: flip rows where tName = rows[;0];
  upd[tName] update `timestamp$timestamp from delete tabName from flip (`tabName, schema[tName][`c])!parsed
 };

updTableIterator: {[fn; rows]
  rows fn/: key schema;
  :1b;
 };
//////////// PUBLIC APIs ////////////

// rows is a list of comma separated string.
// The first element is the name of the table
updSrcPrefixedStrings :updTableIterator updSrcPrefixedStringsSingleTable;

updInflux: updTableIterator updInfluxSingleTable;

updQBinary: updTableIterator updQBinarySingleTable;

updQBinaryMap: updTableIterator {[rowsMap; tName] upd[tName] update `timestamp$timestamp
  from flip schema[tName][`c]!flip rowsMap tName};