package operation

import (
	"github.com/ldcsoftware/qiniu-go-sdk/api.v8/kodocli"
)

// elog is embedded logger
var elog kodocli.Ilog

func SetLogger(logger kodocli.Ilog) {
	elog = logger
	kodocli.SetLogger(logger)
}

func init() {
	if elog == nil {
		elog = kodocli.NewLogger()
	}
}
