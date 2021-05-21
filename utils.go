package txstorage

import (
	"github.com/sony/sonyflake"
	"math/rand"
	"strconv"
	"time"
)

var sf *sonyflake.Sonyflake

func init() {
	sf = sonyflake.NewSonyflake(
		sonyflake.Settings{
			StartTime: time.Unix(1567267200, 0),
		},
	)
	rand.Seed(time.Now().UnixNano())
}

// generateSeqId 生成自增ID
func generateSeqId() string {
	id, _ := sf.NextID()
	return strconv.FormatUint(id, 36)
}
