include "schema/common.q";

// column timestamp comes as a long integer!
schema[`readings]: `c`t!(`timestamp`name`fleet`driver`model`device_version`load_capacity`fuel_capacity`nominal_fuel_consumption`latitude`longitude`elevation`velocity`heading`grade`fuel_consumption;
                   "PSSSSSFFFFFFFFFF");

// column timestamp comes as a long integer!
schema[`diagnostics]: `c`t!(`timestamp`name`fleet`driver`model`device_version`load_capacity`fuel_capacity`nominal_fuel_consumption`fuel_state`current_load`status;
                   "PSSSSSFFFIII");


prtnCol: `timestamp;
sortColsDisk: `name;