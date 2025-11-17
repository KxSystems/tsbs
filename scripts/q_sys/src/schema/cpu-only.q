include "schema/common.q";

// column timestamp comes as a long integer!
schema[`cpu]: `c`t!(`timestamp`hostname`region`datacenter`rack`os`arch`team`service`service_version`service_environment`usage_user`usage_system`usage_idle`usage_nice`usage_iowait`usage_irq`usage_softirq`usage_steal`usage_guest`usage_guest_nice;
                   "PSSSISSSIISIIIIIIIIII");

prtnCol: `timestamp;
sortColsDisk: `hostname;