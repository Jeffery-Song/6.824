package shardmaster

import "raft"
import "labrpc"
import "sync"
import "labgob"
import "time"
import "fmt"

/*
func myDebug(other ...interface{}) {
	fmt.Print(time.Now().String()[14:25], " ")
	fmt.Println(other...)
} // */

// /*
func myDebug(other ...interface{}) {
} // */

const (
	opJoin  int = 0
	opLeave int = 1
	opMove  int = 2
	opQuery int = 3
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	executedSequence map[int64]int // clientId -> maxSeq

	onGoingCtxsMutex sync.Mutex
	onGoingCtxs      map[int]*onGoingCtx

	terminate bool
}

func (sm *ShardMaster) isExecuted(cid int64, seq int) bool {
	maxSeq, isIn := sm.executedSequence[cid]
	if !isIn || maxSeq < seq {
		return false
	}
	return true
}

func (sm *ShardMaster) rebalance(dst *Config, src *Config) {
	if len(dst.Groups) == 0 {
		// directly put them to invalid group 0
		for i := 0; i < len(dst.Shards); i++ {
			dst.Shards[i] = 0
		}
		return
	}
	if len(src.Groups) == 0 {
		// originally there is only group 0, so we do it round robin
		gidList := make([]int, len(dst.Groups))
		gidIdx := 0
		for gid := range dst.Groups {
			gidList[gidIdx] = gid
			gidIdx++
		}
		gidIdx = 0
		for shardIdx := 0; shardIdx < len(dst.Shards); shardIdx++ {
			dst.Shards[shardIdx] = gidList[gidIdx]
			gidIdx = (gidIdx + 1) % len(gidList)
		}
		return
	}

	// count shards for each gid
	gidToNShards := make(map[int]int)
	nShardsOfG0 := 0
	for gid := range dst.Groups {
		gidToNShards[gid] = 0
	}
	for i := 0; i < len(dst.Shards); i++ {
		gid := src.Shards[i]
		if _, notDeleted := dst.Groups[gid]; !notDeleted {
			nShardsOfG0 ++
			dst.Shards[i] = 0
		} else {
			dst.Shards[i] = src.Shards[i]
			gidToNShards[dst.Shards[i]] ++
		}
	}

	for nShardsOfG0 > 0 {
		// fmt.Printf("rebalancing, [0]=%v, %v\n", nShardsOfG0, gidToNShards)
		// find the min
		minGid := 0
		for gid := range dst.Groups {
			minGid = gid
			break
		}
		min := gidToNShards[minGid]
		for gid, nShards := range gidToNShards {
			if nShards < min {
				minGid = gid
				min = nShards
			}
		}
		// put a shard of 0 to minGid
		for shardIdx := 0; shardIdx < len(dst.Shards); shardIdx++ {
			if dst.Shards[shardIdx] == 0 {
				dst.Shards[shardIdx] = minGid
				gidToNShards[minGid] ++
				nShardsOfG0 --
				break
			}
		}
	}

	for {
		// fmt.Printf("rebalancing %v\n", gidToNShards)
		minGid := dst.Shards[0]
		min := gidToNShards[minGid]
		maxGid := dst.Shards[0]
		max := gidToNShards[maxGid]
		for gid, nShards := range gidToNShards {
			if nShards > max {
				maxGid = gid
				max = nShards
			}
			if nShards < min {
				minGid = gid
				min = nShards
			}
		}
		if max-min > 1 {
			// select one from max to min
			for shardIdx := 0; shardIdx < len(dst.Shards); shardIdx++ {
				if dst.Shards[shardIdx] == maxGid {
					dst.Shards[shardIdx] = minGid
					gidToNShards[minGid] ++
					gidToNShards[maxGid] --
					break
				}
			}
		} else {
			break
		}
	}
	return
}

type onGoingCtx struct {
	clientId int64
	seq      int
	op       int // 0 for join, 1 for leave, 2 for move, 3 for query
	reply    interface{}
	wakeUpCh chan interface{}
}

type Op struct {
	// Your data here.
	ClientId  int64
	Sequence  int
	Operation int
	Args      interface{}
}

func (op *Op) String() string {
	ret := "("
	switch op.Operation {
	case opJoin:
		// ret += "Join \"" + op.Args.(*JoinArgs). + "\""
		ret += "Join"
	case opLeave:
		ret += "Leave " + fmt.Sprint(op.Args.(*LeaveArgs).GIDs)
	case opMove:
		ret += "Move shard " + fmt.Sprint(op.Args.(*MoveArgs).Shard) + " to gid=" + fmt.Sprint(op.Args.(*MoveArgs).GID)
	case opQuery:
		ret += "Query config " + fmt.Sprint(op.Args.(*QueryArgs).Num)
	}
	ret += " cid,seq=" + fmt.Sprint(op.ClientId, op.Sequence) + ")"
	return ret
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{}
	op.ClientId = args.ClientId
	op.Sequence = args.Sequence
	op.Operation = opJoin
	op.Args = args
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		myDebug(sm.me, " : a join rpc, but i'm not leader, seq,cid=", args.Sequence, args.ClientId)
		// this is not the leader
		reply.WrongLeader = true
		reply.Err = NotLeader
		return
	}
	myDebug(sm.me, " : a join rpc, cid,seq,idx=", args.ClientId, args.Sequence, index)
	wakeUpCh := make(chan interface{}, 1)
	sm.onGoingCtxsMutex.Lock()
	sm.onGoingCtxs[index] = &onGoingCtx{args.ClientId, op.Sequence, op.Operation, reply, wakeUpCh}
	sm.onGoingCtxsMutex.Unlock()
	// TODO: wait for notification
	<-wakeUpCh
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{}
	op.ClientId = args.ClientId
	op.Sequence = args.Sequence
	op.Operation = opLeave
	op.Args = args
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		myDebug(sm.me, " : a leave rpc, but i'm not leader, seq,cid=", args.Sequence, args.ClientId)
		// this is not the leader
		reply.WrongLeader = true
		reply.Err = NotLeader
		return
	}
	myDebug(sm.me, " : a leave rpc, cid,seq,idx=", args.ClientId, args.Sequence, index)
	wakeUpCh := make(chan interface{}, 1)
	sm.onGoingCtxsMutex.Lock()
	sm.onGoingCtxs[index] = &onGoingCtx{args.ClientId, op.Sequence, op.Operation, reply, wakeUpCh}
	sm.onGoingCtxsMutex.Unlock()
	// TODO: wait for notification
	<-wakeUpCh
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{}
	op.ClientId = args.ClientId
	op.Sequence = args.Sequence
	op.Operation = opMove
	op.Args = args
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		myDebug(sm.me, " : a move rpc, but i'm not leader, seq,cid=", args.Sequence, args.ClientId)
		// this is not the leader
		reply.WrongLeader = true
		reply.Err = NotLeader
		return
	}
	myDebug(sm.me, " : a move rpc, cid,seq,idx=", args.ClientId, args.Sequence, index)
	wakeUpCh := make(chan interface{}, 1)
	sm.onGoingCtxsMutex.Lock()
	sm.onGoingCtxs[index] = &onGoingCtx{args.ClientId, op.Sequence, op.Operation, reply, wakeUpCh}
	sm.onGoingCtxsMutex.Unlock()
	// TODO: wait for notification
	<-wakeUpCh
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{}
	op.ClientId = args.ClientId
	op.Sequence = args.Sequence
	op.Operation = opQuery
	op.Args = args
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		myDebug(sm.me, " : a query rpc, but i'm not leader, seq,cid=", args.Sequence, args.ClientId)
		// this is not the leader
		reply.WrongLeader = true
		reply.Err = NotLeader
		return
	}
	myDebug(sm.me, " : a query rpc, num,cid,seq,idx=", args.Num, args.ClientId, args.Sequence, index)
	wakeUpCh := make(chan interface{}, 1)
	sm.onGoingCtxsMutex.Lock()
	sm.onGoingCtxs[index] = &onGoingCtx{args.ClientId, op.Sequence, op.Operation, reply, wakeUpCh}
	sm.onGoingCtxsMutex.Unlock()
	// TODO: wait for notification
	<-wakeUpCh
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	sm.mu.Lock()
	sm.terminate = true
	sm.mu.Unlock()
	go func() {
		sm.applyCh <- raft.ApplyMsg{}
	}()
	// Your code here, if desired.
}

func (sm *ShardMaster) executeOp(op Op) (int, string) {
	myDebug(sm.me, " : execution, op=", op.String())
	switch op.Operation {
	case opJoin:
		args := op.Args.(*JoinArgs)
		lastConfig := &sm.configs[len(sm.configs)-1]
		newConfig := &Config{}
		newConfig.Num = lastConfig.Num + 1
		newConfig.Groups = make(map[int][]string)
		for gid, servers := range lastConfig.Groups {
			newConfig.Groups[gid] = servers
		}
		for gid, servers := range args.Servers {
			if _, isIn := newConfig.Groups[gid]; isIn {
				// this gid has already been used
				return 0, "Dumplicate GID"
			}
			newConfig.Groups[gid] = servers
		}
		sm.rebalance(newConfig, lastConfig)
		sm.configs = append(sm.configs, *newConfig)
		myDebug(sm.me, "after join, config is", newConfig.Shards)
	case opLeave:
		args := op.Args.(*LeaveArgs)
		lastConfig := &sm.configs[len(sm.configs)-1]
		newConfig := &Config{}
		newConfig.Num = lastConfig.Num + 1
		newConfig.Groups = make(map[int][]string)
		for gid, servers := range lastConfig.Groups {
			newConfig.Groups[gid] = servers
		}
		for _, gidToDelete := range args.GIDs {
			if _, isIn := newConfig.Groups[gidToDelete]; !isIn {
				// this gid has already been used
				return 0, "GID not found"
			}
			delete(newConfig.Groups, gidToDelete)
		}
		sm.rebalance(newConfig, lastConfig)
		sm.configs = append(sm.configs, *newConfig)
		myDebug(sm.me, "after leave, config is", newConfig.Shards)
	case opMove:
		args := op.Args.(*MoveArgs)
		lastConfig := &sm.configs[len(sm.configs)-1]
		if _, isIn := lastConfig.Groups[args.GID]; !isIn {
			return 0, "GID not found"
		}
		newConfig := &Config{}
		newConfig.Num = lastConfig.Num + 1
		newConfig.Groups = make(map[int][]string)
		for gid, servers := range lastConfig.Groups {
			newConfig.Groups[gid] = servers
		}
		newConfig.Shards = lastConfig.Shards
		newConfig.Shards[args.Shard] = args.GID
		sm.configs = append(sm.configs, *newConfig)
		myDebug(sm.me, "after move, config is", newConfig.Shards)
	case opQuery:
		args := op.Args.(*QueryArgs)
		if args.Num == -1 || args.Num >= len(sm.configs) {
			myDebug(sm.me, "executing query num=", args.Num, ", we give idx=", len(sm.configs)-1)
			return len(sm.configs) - 1, ""
		} else {
			myDebug(sm.me, "executing query num=", args.Num, ", we give idx=", args.Num)
			return args.Num, ""
		}
	default:
		// something is wrong
		return 0, "Unknown op"
	}
	return 0, ""
}

func (ctx *onGoingCtx) replyFail() {
	switch ctx.op {
	case opJoin:
		joinReply := ctx.reply.(*JoinReply)
		joinReply.Err = CommitFail
		joinReply.WrongLeader = false
	case opLeave:
		leaveReply := ctx.reply.(*LeaveReply)
		leaveReply.Err = CommitFail
		leaveReply.WrongLeader = false
	case opMove:
		moveReply := ctx.reply.(*MoveReply)
		moveReply.Err = CommitFail
		moveReply.WrongLeader = false
	case opQuery:
		queryReply := ctx.reply.(*QueryReply)
		queryReply.WrongLeader = false
		queryReply.Err = CommitFail
	}
}

func (sm *ShardMaster) replyHandler(op Op, index int, configIdx int, errMsg string) {
	sm.onGoingCtxsMutex.Lock()
	onGoCtx, isMeSentOut := sm.onGoingCtxs[index]
	delete(sm.onGoingCtxs, index)
	sm.onGoingCtxsMutex.Unlock()
	if !isMeSentOut {
		return
	}
	if onGoCtx.seq != op.Sequence || onGoCtx.clientId != op.ClientId {
		// I have an ongoing txn at this index, but not the one raft told me.
		// directly return false
		myDebug(sm.me, " : the index is taken by others, index,op.log,localCtxSeq=", index, op.String(), onGoCtx.seq)
		onGoCtx.replyFail()
		onGoCtx.wakeUpCh <- nil
		return
	}
	switch op.Operation {
	case opJoin: // get
		// serve the read and return the value out
		joinReply := onGoCtx.reply.(*JoinReply)
		// myDebug(sm.me, " : successful op=", op.String(), ", result is \""+val+"\"")
		joinReply.Err = Err(errMsg)
		joinReply.WrongLeader = false
		onGoCtx.wakeUpCh <- nil
	case opLeave: // put
		leaveReply := onGoCtx.reply.(*LeaveReply)
		// myDebug(sm.me, " : successful op=", op.String())
		leaveReply.Err = Err(errMsg)
		leaveReply.WrongLeader = false
		onGoCtx.wakeUpCh <- nil
	case opMove: // append
		moveReply := onGoCtx.reply.(*MoveReply)
		// myDebug(sm.me, " : successful op=", op.String())
		moveReply.Err = Err(errMsg)
		moveReply.WrongLeader = false
		onGoCtx.wakeUpCh <- nil
	case opQuery:
		queryReply := onGoCtx.reply.(*QueryReply)
		queryReply.WrongLeader = false
		queryReply.Err = Err(errMsg)
		queryReply.Config = sm.configs[configIdx]
		if configIdx != queryReply.Config.Num {
			println(sm.me, "different num and idx", queryReply.Config.Num, configIdx)
		}
		onGoCtx.wakeUpCh <- nil
	}
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func applyRoutine(sm *ShardMaster) {
	to := time.NewTimer(time.Second)

	for {
		to.Reset(time.Second)
		var applyMsg raft.ApplyMsg
		select {
		case applyMsg = <-sm.applyCh:
			sm.mu.Lock()
			if sm.terminate == true {
				sm.mu.Unlock()
				myDebug(sm.me, " : exiting")
				return
			}
			sm.mu.Unlock()
			if applyMsg.CommandValid == true {
				op, opOk := applyMsg.Command.(Op)
				if !opOk {
					// something wrong with the entry, should panic
					continue
				}
				myDebug(sm.me, " :found a committed op, idx=", op.String(), applyMsg.CommandIndex)
				configIdx := 0
				errMsg := ""
				// whether executed, they all need reply
				sm.mu.Lock()
				if sm.isExecuted(op.ClientId, op.Sequence) {
					// reply of read need the value
					if op.Operation == opQuery { //get
						configIdx, errMsg = sm.executeOp(op)
					}
				} else {
					configIdx, errMsg = sm.executeOp(op)
					sm.executedSequence[op.ClientId] = op.Sequence
				}
				// may need to notice rpc handler to reply
				sm.replyHandler(op, applyMsg.CommandIndex, configIdx, errMsg)
				sm.mu.Unlock()
			} else {
				// installed snapshot from other one
			}

		case <-to.C:
			sm.onGoingCtxsMutex.Lock()
			for idx, ctx := range sm.onGoingCtxs {
				myDebug(sm.me, " : for too long there is no apply msg, reply fail: idx,seq=", idx, ctx.seq)
				ctx.replyFail()
				ctx.wakeUpCh <- nil
			}
			sm.onGoingCtxs = make(map[int]*onGoingCtx)
			sm.onGoingCtxsMutex.Unlock()
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = make(map[int][]string)
	// sm.configs[0].Groups[0] = nil
	sm.configs[0].Num = 0
	sm.configs[0].Shards = [10]int{0, 0, 0, 0, 0,
		0, 0, 0, 0, 0}

	labgob.Register(Op{})
	labgob.Register(&JoinArgs{})
	labgob.Register(&LeaveArgs{})
	labgob.Register(&MoveArgs{})
	labgob.Register(&QueryArgs{})
	labgob.Register(Config{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.executedSequence = make(map[int64]int)
	sm.onGoingCtxs = make(map[int]*onGoingCtx)
	sm.terminate = false
	// sm.usedGidList = make([]bool, 0)
	// sm.usedGidCount = 0
	go applyRoutine(sm)

	// Your code here.

	return sm
}
