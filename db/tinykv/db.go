package tinykv

import (
	"fmt"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/tikv/client-go/v2/config"
)

const (
	tinykvPD = "tinykv.pd"
	// raw, txn, or coprocessor
	tinykvType      = "tinykv.type"
	tinykvConnCount = "tinykv.conncount"
	tinykvBatchSize = "tinykv.batchsize"
)

type tinykvCreator struct {
}

func (c tinykvCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	config.UpdateGlobal(func(c *config.Config) {
		c.TinyKVClient.GrpcConnectionCount = p.GetUint(tinykvConnCount, 128)
		c.TinyKVClient.MaxBatchSize = p.GetUint(tinykvBatchSize, 128)
	})

	tp := p.GetString(tinykvType, "raw")
	switch tp {
	case "raw":
		return createRawDB(p)
	case "txn":
		return nil, fmt.Errorf("unimplemented type %s", tp)
	default:
		return nil, fmt.Errorf("unsupported type %s", tp)
	}
}

func init() {
	ycsb.RegisterDBCreator("tinykv", tinykvCreator{})
}
