package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "time"
import "fmt"
import "shardmaster"
import "bytes"

const (
	opGet    int = 0
	opPut    int = 1
	opAppend int = 2
	opReconfig int = 3
)

const configQueryDelta int = 1e8

func myDebug(other ...interface{}) {
	/*
	fmt.Print(time.Now().String()[14:25], " ")
	fmt.Println(other...) 
	// */
	// fmt.Print()
} 

type FetchShardArgs struct {
	Shard        int
	NewConfigNum int
}

type FetchShardReply struct {
	Success      bool
	ShardContent *ShardContents 
}

type ReconfigReply struct {
	Err       Err
}
type KvOpArgs struct {
	Shard     int
	Key       string
	Value     string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	Sequence  int // to identify unique operation
	Operation int 
	OpArgs    interface{}
}
func (op *Op) String() string{
	ret := "("
	switch op.Operation {
	case opGet:
		ret += "Get \"" + op.OpArgs.(KvOpArgs).Key + "\""
		ret += fmt.Sprintf(", shard=%v,cid=%v,seq=%v)", op.OpArgs.(KvOpArgs).Shard, op.ClientId, op.Sequence)
	case opPut:
		ret += "Put \"" + op.OpArgs.(KvOpArgs).Key + "\"=\"" + op.OpArgs.(KvOpArgs).Value + "\""
		ret += fmt.Sprintf(", shard=%v,cid=%v,seq=%v)", op.OpArgs.(KvOpArgs).Shard, op.ClientId, op.Sequence)
	case opAppend:
		ret += "Append \"" + op.OpArgs.(KvOpArgs).Key + "\"=\"" + op.OpArgs.(KvOpArgs).Value + "\""
		ret += fmt.Sprintf(", shard=%v,cid=%v,seq=%v)", op.OpArgs.(KvOpArgs).Shard, op.ClientId, op.Sequence)
	}
	return ret
}

type ShardContents struct {
	InUse             bool
	ExecutedSequence  map[int64]int
	Database          map[string]string
}
func (shard *ShardContents) isExecuted(clientId int64, seq int) bool {
	maxSeq, isIn := shard.ExecutedSequence[clientId]; 
	if !isIn || maxSeq < seq {
		return false
	}
	return true
}

func makeShardContent() *ShardContents {
	newShard := &ShardContents{}
	newShard.Database = make(map[string]string)
	newShard.ExecutedSequence = make(map[int64]int)
	// newShard.onGoingCtxs = make(map[int]*onGoingCtx)
	return newShard
}

type onGoingCtx struct {
	clientId int64
	seq      int
	op       int
	shard    int
	reply    interface{}
	wakeUpCh chan interface{}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// op's sequence sent by myself, along with its index returned by raft
	onGoingCtxs       map[int]*onGoingCtx // from index to sequence and *reply
	onGoingCtxsMutex  sync.Mutex

	mck          *shardmaster.Clerk   // talk to shardmaster

	shards       map[int]*ShardContents

	terminate bool

	currentConfig *shardmaster.Config
}

func (kv *ShardKV) genSnapshot() []byte {
	wbuf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(wbuf)
	encoder.Encode(kv.shards)
	encoder.Encode(kv.currentConfig)
	return wbuf.Bytes()
}
func (kv *ShardKV) recoverSnapshot(b []byte) {
	rbuf := bytes.NewBuffer(b)
	decoder := labgob.NewDecoder(rbuf)
	if decoder.Decode(&kv.shards) != nil ||
	   decoder.Decode(&kv.currentConfig) != nil {
		panic("decode snapshot fail")
	}
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// if no such shard, rej
	shard, shardExist := kv.shards[args.Shard]
	// if !shardExist || !shard.inUse {
	// 	kv.mu.Unlock()
	// 	myDebug(kv.gid, kv.me, " : a get rpc, but i'm not the group, cid,seq,shard=",args.ClientId,args.Sequence,args.Shard)
	// 	reply.Err = ErrWrongGroup
	// 	reply.WrongLeader = false
	// 	reply.Value = ""
	// 	return
	// }
	if shardExist && shard.isExecuted(args.ClientId, args.Sequence) {
		value, exist := shard.Database[args.Key]
		kv.mu.Unlock()
		_, isLeader := kv.rf.GetState()
		reply.WrongLeader = !isLeader
		if !exist {
			value = ""
		}
		reply.Value = value
		reply.Err = OK
		return
	}
	kv.mu.Unlock()
	op := Op{}
	op.Sequence = args.Sequence
	op.ClientId = args.ClientId
	op.Operation = opGet
	OpArgs := KvOpArgs{}
	OpArgs.Shard = args.Shard
	OpArgs.Key = args.Key
	op.OpArgs = OpArgs
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		reply.Value = ""
		return
	}
	myDebug(kv.gid, kv.me, " : a get rpc, key,cid,seq,shard,idx=", "\""+args.Key+"\"",args.ClientId, args.Sequence, args.Shard, index)
	wakeUpCh := make(chan interface{}, 1)
	ongoingCtx := &onGoingCtx{
		args.ClientId,
		args.Sequence,
		op.Operation,
		args.Shard,
		reply,
		wakeUpCh,
	}
	kv.onGoingCtxsMutex.Lock()
	kv.onGoingCtxs[index] = ongoingCtx
	kv.onGoingCtxsMutex.Unlock()
	<-wakeUpCh
	myDebug(kv.gid, kv.me, " : get rpc returns, key,cid,seq,shard,idx=", "\""+args.Key+"\"",args.ClientId, args.Sequence,args.Shard, index)
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// if no such shard, rej
	shard, shardExist := kv.shards[args.Shard]
	// if !shardExist || !shard.inUse {
	// 	kv.mu.Unlock()
	// 	myDebug(kv.gid, kv.me, " : a putappend rpc, but i'm not the group, cid,seq,shard=", args.ClientId,args.Sequence, args.Shard)
	// 	reply.Err = ErrWrongGroup
	// 	reply.WrongLeader = false
	// 	return
	// }
	if shardExist && shard.isExecuted(args.ClientId, args.Sequence) {
		kv.mu.Unlock()
		_, isLeader := kv.rf.GetState()
		reply.WrongLeader = !isLeader
		reply.Err = OK
		return
	}
	kv.mu.Unlock()
	op := Op{}
	op.Sequence = args.Sequence
	op.ClientId = args.ClientId
	if args.Op == "Put" {
		op.Operation = opPut
	} else if args.Op == "Append" {
		op.Operation = opAppend
	}
	OpArgs := KvOpArgs{}
	OpArgs.Shard = args.Shard
	OpArgs.Key = args.Key
	OpArgs.Value = args.Value
	op.OpArgs = OpArgs
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}
	myDebug(kv.gid, kv.me, " : a putappend rpc, key,cid,seq,shard,idx=", "\""+args.Key+"\"",args.ClientId, args.Sequence, args.Shard, index)
	wakeUpCh := make(chan interface{}, 1)
	ongoingCtx := &onGoingCtx{
		args.ClientId,
		args.Sequence,
		op.Operation,
		args.Shard,
		reply,
		wakeUpCh,
	}
	kv.onGoingCtxsMutex.Lock()
	kv.onGoingCtxs[index] = ongoingCtx
	kv.onGoingCtxsMutex.Unlock()
	<-wakeUpCh
	myDebug(kv.gid, kv.me, " : putappend rpc returns, key,cid,seq,shard,idx=", "\""+args.Key+"\"",args.ClientId, args.Sequence,args.Shard, index)
	return
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.mu.Lock()
	kv.terminate = true
	kv.mu.Unlock()
	go func() {
		kv.applyCh <- raft.ApplyMsg{}
	} ()
	// Your code here, if desired.
}

// return the ctx if outside should reply success
func (kv *ShardKV) findCtx(me int, op Op, index int) *onGoingCtx {
	kv.onGoingCtxsMutex.Lock()
	onGoCtx, isMeSentOut := kv.onGoingCtxs[index]
	delete(kv.onGoingCtxs, index)
	kv.onGoingCtxsMutex.Unlock()
	if !isMeSentOut {
		return nil
	}
	if onGoCtx.seq != op.Sequence || onGoCtx.clientId != op.ClientId {
		// I have an ongoing txn at this index, but not the one raft told me.
		// directly return false
		myDebug(me, " : the index is taken by others, index,op.log,localCtxSeq=", index, op.String(), onGoCtx.seq)
		onGoCtx.replyFail(ErrCommitFail)
		onGoCtx.wakeUpCh <- nil
		return nil
	}
	return onGoCtx
}

func (kv *ShardKV) executeAndReplyGet(op Op, index int) {
	// execution
	myDebug(kv.gid, kv.me, " : execution, op=", op.String())
	kv.mu.Lock()
	kvOpArgs := op.OpArgs.(KvOpArgs)
	shard, shardExist := kv.shards[kvOpArgs.Shard]
	err := OK
	retVal := ""
	if !shardExist || !shard.InUse {
		// todo: reply wrong shard
		err = ErrWrongGroup
	} else {
		val, exist := shard.Database[kvOpArgs.Key]
		if exist {
			retVal = val
		}
		myDebug(kv.gid, kv.me, " : get result is, op,value=", op.String(), ",\""+retVal+"\"")
		// part in applyRoutine
		if !shard.isExecuted(op.ClientId, op.Sequence) {
			shard.ExecutedSequence[op.ClientId] = op.Sequence
		}
	}
	kv.mu.Unlock()
	// replyHandler
	onGoCtx := kv.findCtx(kv.me, op, index)
	if onGoCtx == nil {
		return
	}
	myDebug(kv.gid, kv.me, " : get result should be replied, idx,op=",index, op.String())
	getReply := onGoCtx.reply.(*GetReply)
	getReply.WrongLeader = false
	getReply.Value = retVal
	if err != OK {
		getReply.Err = err
		myDebug(kv.gid, kv.me, " : failed idx,op=", index, op.String(), ", err is \""+err+"\"")
	} else {
		myDebug(kv.gid, kv.me, " : successful idx,op=", index, op.String(), ", result is \""+retVal+"\"")
		getReply.Err = OK
	}
	onGoCtx.wakeUpCh <- nil
}

func (kv *ShardKV) executeAndReplyPut(op Op, index int) {
	kvOpArgs := op.OpArgs.(KvOpArgs)
	kv.mu.Lock()
	shard, shardExist := kv.shards[kvOpArgs.Shard]
	err := OK
	if !shardExist || !shard.InUse {
		// todo: reply wrong shard
		err = ErrWrongGroup
	} else {
		if !shard.isExecuted(op.ClientId, op.Sequence) {
			myDebug(kv.gid, kv.me, " : execution, op=", op.String())
			shard.Database[kvOpArgs.Key] = kvOpArgs.Value
			myDebug(kv.gid, kv.me, " : put result is, op,value=", op.String(), ",\""+shard.Database[kvOpArgs.Key]+"\"")
			shard.ExecutedSequence[op.ClientId] = op.Sequence
		}
	}
	kv.mu.Unlock()
	// replyHandler
	onGoCtx := kv.findCtx(kv.me, op, index)
	if onGoCtx == nil {
		return
	}
	putAppReply := onGoCtx.reply.(*PutAppendReply)
	putAppReply.WrongLeader = false
	if err != OK {
		putAppReply.Err = err
	} else {
		myDebug(kv.gid, kv.me, " : successful op=", op.String())
		putAppReply.Err = OK
	}
	onGoCtx.wakeUpCh <- nil
}

func (kv *ShardKV) executeAndReplyAppend(op Op, index int) {
	kvOpArgs := op.OpArgs.(KvOpArgs)
	kv.mu.Lock()
	shard, shardExist := kv.shards[kvOpArgs.Shard]
	err := OK
	if !shardExist || !shard.InUse {
		// todo: reply wrong shard
		err = ErrWrongGroup
	} else {
		if !shard.isExecuted(op.ClientId, op.Sequence) {
			myDebug(kv.gid, kv.me, " : execution, op=", op.String())
			shard.Database[kvOpArgs.Key] += kvOpArgs.Value
			myDebug(kv.gid, kv.me, " : append result is, op,value=", op.String(), ",\""+shard.Database[kvOpArgs.Key]+"\"")
			shard.ExecutedSequence[op.ClientId] = op.Sequence
		}
	}
	kv.mu.Unlock()
	// replyHandler
	onGoCtx := kv.findCtx(kv.me, op, index)
	if onGoCtx == nil {
		return
	}
	putAppReply := onGoCtx.reply.(*PutAppendReply)
	putAppReply.WrongLeader = false
	if err != OK {
		putAppReply.Err = err
	} else {
		myDebug(kv.gid, kv.me, " : successful op=", op.String())
		putAppReply.Err = OK
	}
	onGoCtx.wakeUpCh <- nil
}

func (kv *ShardKV) executeReconfig(op Op, index int) {
	// reconfig
	// todo: lock
	config := op.OpArgs.(shardmaster.Config)
	err := OK
	kv.mu.Lock()
	if config.Num != kv.currentConfig.Num + 1 {
		kv.mu.Unlock()
		err = ErrReconfigDisorder
	} else {
		myDebug(kv.gid, kv.me, "executing reconfig, num=", config.Num)
		fetchDoneCh := make(map[int]chan *ShardContents, 1)
		for shard, gid := range config.Shards {
			if kv.currentConfig.Shards[shard] == gid {
				continue
			}
			if gid == kv.gid { // new shard comming to me
				if kv.currentConfig.Num == 0 || kv.currentConfig.Shards[shard] == 0 {
					// brand new shard, just create it myself
					myDebug(kv.gid, kv.me, "brand new shard", shard)
					newShard := makeShardContent()
					newShard.InUse = true
					kv.shards[shard] = newShard
				} else { // request other group to get the shard
					myDebug(kv.gid, kv.me, "a new shard to fetch from, shard,gid=", shard, kv.currentConfig.Shards[shard])
					fetchDoneCh[shard] = make(chan *ShardContents)
					servers := kv.currentConfig.Groups[kv.currentConfig.Shards[shard]]
					go func(shard int, ch chan *ShardContents, servers []string) {
						i := 0
						for {
							args := &FetchShardArgs{}
							args.Shard = shard
							args.NewConfigNum = config.Num
							reply := &FetchShardReply{}
							ok := kv.make_end(servers[i]).Call("ShardKV.FetchShard", args, reply)
							if !ok || !reply.Success {
								time.Sleep(time.Duration(1e8))
								i = (i + 1) % len(servers)
								continue
							}
							myDebug(kv.gid, kv.me, "fetched shard", shard)
							ch <- reply.ShardContent
							return
						}
					} (shard, fetchDoneCh[shard], servers)
				}
			} else if kv.currentConfig.Shards[shard] == kv.gid {
				myDebug(kv.gid, kv.me, "disabling shard", shard)
				kv.shards[shard].InUse = false
			}
		}
		kv.mu.Unlock()
		for shard, ch := range fetchDoneCh {
			sc := <- ch
			kv.mu.Lock()
			kv.shards[shard] = sc
			kv.mu.Unlock()
		}
		myDebug(kv.gid, kv.me, "all shards fetched, num=", config.Num)
		kv.mu.Lock()
		kv.currentConfig = &config
		for shard := range fetchDoneCh {
			kv.shards[shard].InUse = true
		}
		kv.mu.Unlock()
	}
	// reply
	onGoCtx := kv.findCtx(kv.me, op, index)
	if onGoCtx == nil {
		return
	}
	reconfigReply := onGoCtx.reply.(*ReconfigReply)
	reconfigReply.Err = err
	onGoCtx.wakeUpCh <- nil
}

func (kv *ShardKV) FetchShard(args *FetchShardArgs, reply *FetchShardReply) {
	kv.mu.Lock()
	if kv.currentConfig.Num < args.NewConfigNum {
		myDebug(kv.gid, kv.me, "rejecting fetchshard due to old local config, local,new=", args.NewConfigNum)
		kv.mu.Unlock()
		reply.Success = false
		return
	}
	reply.Success = true
	myDebug(kv.gid, kv.me, "grant fetchshard, local,new=", kv.currentConfig.Num, args.NewConfigNum)
	reply.ShardContent = &ShardContents{}
	reply.ShardContent.InUse =false
	reply.ShardContent.ExecutedSequence = make(map[int64]int)
	reply.ShardContent.Database = make(map[string]string)
	for cid, maxSeq := range kv.shards[args.Shard].ExecutedSequence {
		reply.ShardContent.ExecutedSequence[cid] = maxSeq
	}
	for key, val := range kv.shards[args.Shard].Database {
		reply.ShardContent.Database[key] = val
	}
	kv.mu.Unlock()
}


func (ctx *onGoingCtx) replyFail(errMsg Err) {
	switch ctx.op {
	case opGet:
		getReply:= ctx.reply.(*GetReply)
		getReply.Value = ""
		getReply.Err = errMsg
		getReply.WrongLeader = false
	case opPut:
		putAppReply := ctx.reply.(*PutAppendReply)
		putAppReply.Err = errMsg
		putAppReply.WrongLeader = false
	case opAppend:
		putAppReply := ctx.reply.(*PutAppendReply)
		putAppReply.Err = errMsg
		putAppReply.WrongLeader = false
	case opReconfig:
		reconfigReply := ctx.reply.(*ReconfigReply)
		reconfigReply.Err = errMsg
		// todo
	}
}

func applyRoutine(kv *ShardKV) {
	// myDebug(kv.gid, kv.me, "start applying routine")
	to := time.NewTimer(time.Second)
	for {
		// myDebug(kv.gid, kv.me, "waiting for an applyMsg")
		to.Reset(time.Second)
		var applyMsg raft.ApplyMsg
		select {
		case applyMsg = <- kv.applyCh:
			kv.mu.Lock()
			if kv.terminate == true {
				kv.mu.Unlock()
				myDebug(kv.gid, kv.me, " : exiting")
				return
			}
			kv.mu.Unlock()
			if applyMsg.CommandValid == true {
				op, opOk := applyMsg.Command.(Op)
				if !opOk { // something wrong with the entry, should panic
					continue
				}
				myDebug(kv.gid, kv.me, " :found a committed op, idx=", op.String(), applyMsg.CommandIndex)
				switch op.Operation {
				case opGet:
					kv.executeAndReplyGet(op, applyMsg.CommandIndex)
				case opPut:
					kv.executeAndReplyPut(op, applyMsg.CommandIndex)
				case opAppend:
					kv.executeAndReplyAppend(op, applyMsg.CommandIndex)
				case opReconfig:
					kv.executeReconfig(op, applyMsg.CommandIndex)
				}
				kv.mu.Lock()
				if kv.maxraftstate != -1 && kv.maxraftstate < applyMsg.PersistStateSize {
					// todo: snapshot
					kvsnapshot := kv.genSnapshot()
					kv.rf.DoSnapshot(kvsnapshot, applyMsg.CommandIndex)
				}
				kv.mu.Unlock()
			} else {
				// installed snapshot from other one
				// todo: reply all them fail
				kv.mu.Lock()
				kv.recoverSnapshot(applyMsg.Snapshot)
				kv.mu.Unlock()
				kv.onGoingCtxsMutex.Lock()
				for idx, ctx := range kv.onGoingCtxs {
					myDebug(kv.gid, kv.me, "clearing ongoing ctx due to new installed snapshot: idx,seq=", idx, ctx.seq)
					ctx.replyFail(ErrCommitFail)
					ctx.wakeUpCh <- nil
				}
				kv.onGoingCtxs = make(map[int]*onGoingCtx)
				kv.onGoingCtxsMutex.Unlock()
			}

		case <- to.C:
			// myDebug(kv.gid, kv.me, "wait for applyMsg timeout")
			kv.mu.Lock()
			if kv.terminate == true {
				kv.mu.Unlock()
				myDebug(kv.gid, kv.me, " : exiting")
				return
			}
			kv.mu.Unlock()
			// todo: reply all them fail
			kv.onGoingCtxsMutex.Lock()
			for idx, ctx := range kv.onGoingCtxs {
				myDebug(kv.gid, kv.me, " : for too long there is no apply msg, reply fail: idx,seq=", idx, ctx.seq)
				ctx.replyFail(ErrCommitFail)
				ctx.wakeUpCh <- nil
			}
			kv.onGoingCtxs = make(map[int]*onGoingCtx)
			kv.onGoingCtxsMutex.Unlock()
		}
	}
}

func queryConfigRoutine(kv *ShardKV) {
	// myDebug(kv.gid, kv.me, "start query config")
	for {
		kv.mu.Lock()
		if kv.terminate == true {
			kv.mu.Unlock()
			myDebug(kv.gid, kv.me, " : exiting")
			return
		}
		kv.mu.Unlock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(time.Duration(configQueryDelta))
			continue
		}
		kv.mu.Lock()
		currentNum := kv.currentConfig.Num
		kv.mu.Unlock()
		config := kv.mck.Query(currentNum + 1)
		if config.Num == currentNum {
			// myDebug(kv.gid, kv.me, "no new config", config.Shards)
			time.Sleep(time.Duration(configQueryDelta))
			continue
		} else if config.Num != currentNum + 1 {
			panic("discrete config number")
		}
		myDebug(kv.gid, kv.me, "next config found", config.Shards)
		op := Op{} // new config coming, replicate it out
		op.Operation = opReconfig
		op.ClientId = 0
		op.Sequence = config.Num
		op.OpArgs = config
		index, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			time.Sleep(time.Duration(configQueryDelta))
			continue
		}
		myDebug(kv.gid, kv.me, " : replicating a config, num,idx=", config.Num, index)
		wakeUpCh := make(chan interface{}, 1)
		reply := &ReconfigReply{}
		kv.onGoingCtxsMutex.Lock()
		kv.onGoingCtxs[index] = &onGoingCtx{0, op.Sequence, op.Operation, -1, reply, wakeUpCh}
		kv.onGoingCtxsMutex.Unlock()
		<-wakeUpCh
		myDebug(kv.gid, kv.me, " : replication of a config returns, num,idx=", config.Num, index)
		switch reply.Err {
		case OK:
			// ok means the config has been applied.
			continue
		default :
			myDebug("Err str: \""+reply.Err+"\"")
			time.Sleep(time.Duration(configQueryDelta))
			continue
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(KvOpArgs{})
	labgob.Register(&ShardContents{})
	// labgob.Register(ShardContents{})
	labgob.Register(&FetchShardArgs{})
	labgob.Register(&FetchShardReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.currentConfig = &shardmaster.Config{}
	kv.currentConfig.Num = 0
	kv.currentConfig.Groups = make(map[int][]string)
	kv.onGoingCtxs = make(map[int]*onGoingCtx)
	kv.shards = make(map[int]*ShardContents)

	go queryConfigRoutine(kv)
	go applyRoutine(kv)

	return kv
}
