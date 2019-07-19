package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                Err = "OK"
	ErrNoKey          Err = "ErrNoKey"
	ErrWrongGroup     Err = "ErrWrongGroup"
	ErrNotLeader      Err = "Not Leader"
	ErrWrongOperation Err = "Wrong Operation"
	ErrCommitFail     Err = "Commit Failure"
	ErrReconfigDisorder Err = "Reconfig Disorder"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	Sequence int
	ClientId int64
	Shard    int
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Sequence int
	ClientId int64
	Shard    int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
