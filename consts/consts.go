package consts

const (
	DataDir = "/data/"

	//DataDir   = "D:\\test\\ecs\\"
)

type (
	OP   byte
	TYPE byte
)

const (
	Add OP = iota
	Del
	ZAdd
	ZRmv
)

const (
	String TYPE = iota
	Int
)

var (
	KeyBytes   = []byte{'{', '"', 'k', 'e', 'y', '"', ':', '"'}
	ValueBytes = []byte{'"', ',', '"', 'v', 'a', 'l', 'u', 'e', '"', ':', '"'}
	EndBytes   = []byte{'"', '}', ','}

	ScoreBytes      = []byte{'{', '"', 's', 'c', 'o', 'r', 'e', '"', ':'}
	ScoreValueBytes = []byte{',', '"', 'v', 'a', 'l', 'u', 'e', '"', ':', '"'}
)
