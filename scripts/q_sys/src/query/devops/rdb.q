single_groupby_1_by_minute: {[H; st; et]
    select max usage_user by 0D00:01 xbar timestamp
        from cpu where hostname in H, timestamp within (st; et)};

single_groupby_5_by_minute: {[H; st; et]
    select max usage_user, max usage_system, max usage_idle, max usage_nice, max usage_iowait by 0D00:01 xbar timestamp
        from cpu where hostname in H, timestamp within (st; et)};

cpu_max_by_hour: {[H; st; et]
  select max usage_user, max usage_system, max usage_idle, max usage_nice, max usage_iowait, max usage_irq,
         max usage_softirq, max usage_steal, max usage_guest by 0D01 xbar timestamp
       from cpu where hostname in H, timestamp within (st; et)};

double_groupby_1: {[st; et] select avg usage_user by 0D01 xbar timestamp, hostname
       from cpu where timestamp within (st; et)};

double_groupby_5: {[st; et]
    select avg usage_user, avg usage_system, avg usage_idle, avg usage_nice, avg usage_iowait by 0D01 xbar timestamp, hostname
       from cpu where timestamp within (st; et)};


double_groupby_all: {[st; et]
    select avg usage_user, avg usage_system, avg usage_idle, avg usage_nice, avg usage_iowait,
           avg usage_irq, avg usage_softirq, avg usage_steal, avg usage_guest, avg usage_guest_nice by 0D01 xbar timestamp, hostname
       from cpu where timestamp within (st; et)};

high_cpu_all: {[st; et]
    select from cpu where timestamp within (st; et), usage_user > 90};

high_cpu: {[H; st; et]
    select from cpu where hostname in H, timestamp within (st; et), usage_user > 90};

lastpoint: {[] select by hostname from cpu};

groupby_orderby_limit: {[et]
    st: 0D00:01 xbar et - 0D00:04;
    select max usage_user by 0D00:01 xbar timestamp from cpu where timestamp within (st; et)}
