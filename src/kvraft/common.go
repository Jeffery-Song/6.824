package raftkv

/*
import "fmt"
import "time"
func myDebug(other ...interface{}) {
	fmt.Print(time.Now().String()[14:25], " ")
	fmt.Println(other...) 
} // */

// /*
func myDebug(other ...interface{}) {
} // */

const (
	OK       = "OK" // not used
	ErrNoKey = "ErrNoKey" // not used
	NotLeader = "Not Leader"
	WrongOperation = "Wrong Operation"
	CommitFail = "Commit Failure"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Sequence int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	Sequence int
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
