initLog: {[comp]
  $[`com_kx_log in key `; [
    logfile: hsym `$"fd:///tmp/", string[comp], ".log";
    id: .com_kx_log.init[`:fd://stdout, logfile; `INFO`];
    .qlog:.com_kx_log.new[comp; ()];];
  [
    .qlog.outp:{-1(string .z.z)," ",x;};
    .qlog.error:{.qlog.outp"\033[41;37m",x,"\033[0m";};
    .qlog.warn:{.qlog.outp"\033[43;37m",x,"\033[0m";};
    .qlog.info:{.qlog.outp"\033[42;37m",x,"\033[0m";};
    .qlog.debug: .qlog.outp]];
  };