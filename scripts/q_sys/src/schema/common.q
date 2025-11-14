schema: (`$())!();

createEmptyTable: {[tName]
  tName set flip schema[tName][`c]!schema[tName][`t] $\: ();
  };