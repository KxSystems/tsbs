package kdb

import (
	"github.com/blagojts/viper"
	"github.com/questdb/tsbs/pkg/data/serialize"
	"github.com/questdb/tsbs/pkg/data/source"
	"github.com/questdb/tsbs/pkg/targets"
	"github.com/questdb/tsbs/pkg/targets/constants"
	"github.com/spf13/pflag"
)

func NewTarget() targets.ImplementedTarget {
	return &kdbTarget{}
}

type kdbTarget struct {
}

func (t *kdbTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	pflag.Bool("influx-format", false, "Whether to send data to endpoint with influx line protocol.")
	flagSet.String("data-format", "string", "Data format to send to KDB server. Must be one of: string, binary, binaryDict")
}

func (t *kdbTarget) TargetName() string {
	return constants.FormatKDB
}

func (t *kdbTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *kdbTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
