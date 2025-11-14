$["simple" ~ lower getenv `QUERYTYPE; [
  single_groupby_1_by_minute: {[H; st; et]
    select max_usage_user: max usage_user by 0D00:01 xbar timestamp
        from cpu where date in distinct `date$(st, et), hostname in H, timestamp within (st; et)};

  single_groupby_5_by_minute: {[H; st; et]
    select max_usage_user: max usage_user, max_usage_system: max usage_system, max_usage_idle: max usage_idle,
      max_usage_nice: max usage_nice, max_usage_iowait: max usage_iowait by minute: 0D00:01 xbar timestamp
        from cpu where date in distinct `date$(st, et), hostname in H, timestamp within (st; et)};


  cpu_max_by_hour: {[H; st; et]
    select max_usage_user: max usage_user, max_usage_system: max usage_system, max_usage_idle: max usage_idle,
         max_usage_nice: max usage_nice, max_usage_iowait: max usage_iowait, max_usage_irq: max usage_irq,
         max_usage_softirq: max usage_softirq, max_usage_steal: max usage_steal, max_usage_guest: max usage_guest, max_usage_guest_nice: max usage_guest_nice
       by hour: 0D01 xbar timestamp from cpu where date in distinct `date$(st, et), hostname in H, timestamp within (st; et)}
 ]; [
  single_groupby_1_by_minute: {[H; st; et]
    max {[st; et; h] select max_usage_user: max usage_user by minute: 0D00:01 xbar timestamp
        from cpu where date in distinct `date$(st, et), hostname=h, timestamp within (st; et)}[st; et] peach H};

  single_groupby_5_by_minute: {[H; st; et]
    max {[st; et; h] select max_usage_user: max usage_user, max_usage_system: max usage_system, max_usage_idle: max usage_idle,
      max_usage_nice: max usage_nice, max_usage_iowait: max usage_iowait by minute: 0D00:01 xbar timestamp
        from cpu where date in distinct `date$(st, et), hostname=h, timestamp within (st; et)}[st; et] peach H};

  cpu_max_by_hour: {[H; st; et]
    max {[st; et; h] select max_usage_user: max usage_user, max_usage_system: max usage_system, max_usage_idle: max usage_idle,
         max_usage_nice: max usage_nice, max_usage_iowait: max usage_iowait, max_usage_irq: max usage_irq,
         max_usage_softirq: max usage_softirq, max_usage_steal: max usage_steal, max_usage_guest: max usage_guest, max_usage_guest_nice: max usage_guest_nice
      by hour: 0D01 xbar timestamp from cpu where date in distinct `date$(st, et), hostname=h, timestamp within (st; et)}[st; et] peach H}
 ]];

/ lastpoint: {[] `hostname`time xcol delete date from .Q.fc[{select by hostname from cpu where date=last date, hostname in x}]
/     first value flip select distinct hostname from cpu where date=last date};
lastpoint: {[] `hostname`time xcol delete date from select by hostname from cpu where date=last date};

high_cpu: {[H; st; et]
    `hostname`time xcol delete date from select from cpu where date in distinct `date$(st, et), hostname in H, timestamp within (st; et), usage_user > 90};

high_cpu_all: {[st; et]
    `hostname`time xcol delete date from select from cpu where date in distinct `date$(st, et), timestamp within (st; et), usage_user > 90};

double_groupby_1: {[st; et] select mean_usage_user: avg usage_user by hour: 0D01 xbar timestamp, hostname
       from cpu where date in distinct `date$(st, et), timestamp within (st; et)};

double_groupby_5: {[st; et]
    select mean_usage_user: avg usage_user, mean_usage_system: avg usage_system, mean_usage_idle: avg usage_idle,
      mean_usage_nice: avg usage_nice, mean_usage_iowait: avg usage_iowait by hour: 0D01 xbar timestamp, hostname
       from cpu where date in distinct `date$(st, et), timestamp within (st; et)};

double_groupby_all: {[st; et]
    select mean_usage_user: avg usage_user, mean_usage_system: avg usage_system, mean_usage_idle: avg usage_idle, mean_usage_nice: avg usage_nice,
      mean_usage_iowait: avg usage_iowait, mean_usage_irq: avg usage_irq, mean_usage_softirq: avg usage_softirq, mean_usage_steal: avg usage_steal,
      mean_usage_guest: avg usage_guest, mean_usage_guest_nice: avg usage_guest_nice by hour: 0D01 xbar timestamp, hostname
       from cpu where date in distinct `date$(st, et), timestamp within (st; et)};

groupby_orderby_limit: {[et]
    -5 sublist select max_usage_user: max usage_user by minute: 0D00:01 xbar timestamp from cpu where date=`date$et, timestamp < et
    }
