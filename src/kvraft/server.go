package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"fmt"
	"time"
)

const Debug = 0



func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Sequence int // to identify unique operation
	Operation int // 0 for read, 1 for put, 2 for append
	Key string
	Value string
}
func (op *Op) String() string{
	ret := "("
	switch op.Operation {
	case 0:
		ret += "Get \"" + op.Key + "\""
	case 1:
		ret += "Put \"" + op.Key + "\"=\"" + op.Value + "\""
	case 2:
		ret += "Append \"" + op.Key + "\"=\"" + op.Value + "\""
	}
	ret += " ,seq=" + fmt.Sprint(op.Sequence) + ")"
	// fmt.Sprint(op.Sequence)
	return ret
}

type onGoingCtx struct {
	seq   int
	op    int
	reply interface{}
	wakeUpCh chan interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	
	// sequence of op that has already been executed
	// to prevent dumplicated execution
	// this is thread safe
	executedSequence map[int]bool

	// the database it self
	// this is thread safe
	database map[string]string

	// op's sequence sent by myself, along with its index returned by raft
	// rw sync.RWMutex
	onGoingCtxs map[int]*onGoingCtx // from index to sequence and *reply

	terminate bool
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{}
	op.Sequence = args.Sequence // not used for readonly txn
	op.Key = args.Key
	op.Operation = 0
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		myDebug(kv.me, " : a get rpc, but i'm not leader, seq=" + fmt.Sprint(args.Sequence))
		// this is not the leader
		reply.WrongLeader = true
		reply.Err = NotLeader
		reply.Value = ""
		return
	}
	myDebug(kv.me, " : a get rpc, key,seq,idx=", "\""+args.Key+"\"", args.Sequence, index)
	wakeUpCh := make(chan interface{}, 1)
	kv.mu.Lock()
	kv.onGoingCtxs[index] = &onGoingCtx{op.Sequence, op.Operation, reply, wakeUpCh}
	kv.mu.Unlock()
	// TODO: wait for notification
	<-wakeUpCh
	myDebug(kv.me, " : get rpc returns, key,seq,idx=", "\""+args.Key+"\"", args.Sequence, index)
	
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	op.Sequence = args.Sequence // not used for readonly txn
	op.Key = args.Key
	op.Value = args.Value
	if args.Op == "Put" {
		op.Operation = 1
	} else if args.Op == "Append" {
		op.Operation = 2
	} else {
		// wrong operation
		reply.Err = WrongOperation
		reply.WrongLeader = false
		return
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		myDebug(kv.me, " : a putappend rpc, but i'm not leader")
		// this is not the leader
		reply.WrongLeader = true
		reply.Err = NotLeader
		return
	}
	myDebug(kv.me, " : a putappend rpc, op,key,seq,idx=", args.Op,"\""+args.Key+"\"", args.Sequence, index)
	wakeUpCh := make(chan interface{}, 1)
	kv.mu.Lock()
	kv.onGoingCtxs[index] = &onGoingCtx{op.Sequence, op.Operation, reply, wakeUpCh}
	kv.mu.Unlock()
	// TODO: wait for notification
	<-wakeUpCh
	myDebug(kv.me, " : putappend rpc returns, op,key,seq,idx=", args.Op, "\""+args.Key+"\"", args.Sequence, index)
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.terminate = true
	go func() {
		kv.applyCh <- raft.ApplyMsg{}
	} ()
}

func (kv *KVServer) executeOp(op Op) string {
	myDebug(kv.me, " : execution, op=", op.String())
	switch op.Operation {
	case 0: // get
		value, exist := kv.database[op.Key]
		if !exist {
			value = ""
		}
		myDebug(kv.me, " : get result is, op,value=", op.String(), ",\""+value+"\"")
		return value
	case 1: // put
		kv.database[op.Key] = op.Value
		myDebug(kv.me, " : put result is, op,value=", op.String(), ",\""+kv.database[op.Key]+"\"")
		return ""
	case 2: // append
		kv.database[op.Key] += op.Value
		myDebug(kv.me, " : append result is, op,value=", op.String(), ",\""+kv.database[op.Key]+"\"")
		return ""
	default:
		// something is wrong
		return ""
	}
}

func (ctx *onGoingCtx) replyFail() {
	switch ctx.op {
	case 0:
		getReply:= ctx.reply.(*GetReply)
		getReply.Value = ""
		getReply.Err = CommitFail
		getReply.WrongLeader = false
	case 1:
		putAppReply := ctx.reply.(*PutAppendReply)
		putAppReply.Err = CommitFail
		putAppReply.WrongLeader = false
	case 2:
		putAppReply := ctx.reply.(*PutAppendReply)
		putAppReply.Err = CommitFail
		putAppReply.WrongLeader = false
	}
}

func (kv *KVServer) replyHandler(op Op, index int, val string) {
	kv.mu.Lock()
	onGoCtx, isMeSentOut := kv.onGoingCtxs[index]
	delete(kv.onGoingCtxs, index)
	kv.mu.Unlock()
	if !isMeSentOut {
		return
	}
	if onGoCtx.seq != op.Sequence {
		// I have an ongoing txn at this index, but not the one raft told me.
		// directly return false
		myDebug(kv.me, " : the index is taken by others, index,op.log,localCtxSeq=", index, op.String(), onGoCtx.seq)
		onGoCtx.replyFail()
		onGoCtx.wakeUpCh <- nil
		return
	}
	switch op.Operation {
	case 0: // get
		// serve the read and return the value out
		getReply := onGoCtx.reply.(*GetReply)
		myDebug(kv.me, " : successful op=", op.String(), ", result is \""+val+"\"")
		getReply.Value = val
		getReply.Err = ""
		getReply.WrongLeader = false
		onGoCtx.wakeUpCh <- nil
		
	case 1: // put
		putAppReply := onGoCtx.reply.(*PutAppendReply)
		myDebug(kv.me, " : successful op=", op.String())
		putAppReply.Err = ""
		putAppReply.WrongLeader = false
		onGoCtx.wakeUpCh <- nil
	case 2: // append
		putAppReply := onGoCtx.reply.(*PutAppendReply)
		myDebug(kv.me, " : successful op=", op.String())
		putAppReply.Err = ""
		putAppReply.WrongLeader = false
		onGoCtx.wakeUpCh <- nil
	}
}

func applyRoutine(kv *KVServer) {
	to := time.NewTimer(time.Second)
	// timeoutCh := make(chan int, 1)
	
	for {
		to.Reset(time.Second)
		// applyMsg := <-kv.applyCh
		var applyMsg raft.ApplyMsg
		select {
		case applyMsg = <- kv.applyCh:
			if kv.terminate == true {
				myDebug(kv.me, " : exiting")
				return
			}
			if applyMsg.CommandValid == true {
				// this is a command
				op, opOk := applyMsg.Command.(Op)
				if !opOk {
					// something wrong with the entry, should panic
					continue
				}
				myDebug(kv.me, " :found a committed op, idx=", op.String(), applyMsg.CommandIndex)
				// op is an Op
				val := "" // for potential get
				if _, executed := kv.executedSequence[op.Sequence]; !executed{
					// this command is not executed
					kv.executedSequence[op.Sequence] = true
					val = kv.executeOp(op)
				} else {
					// this command is executed. But the previous execution may fail to 
					// return success, so we follow the normal process to return success
					// or fail if the index is wrong
					if op.Operation == 0 {//get
						val = kv.executeOp(op)
					}
				}
				// may need to notice rpc handler to reply
				kv.replyHandler(op, applyMsg.CommandIndex, val)
			} else {
				// this is another whatever entry in log
				// also need to check op, if there is invalid command
			}
		case <- to.C:
			// _, isLeader := kv.rf.GetState()
			// if !isLeader {
			kv.mu.Lock()
			for idx, ctx := range kv.onGoingCtxs {
				myDebug(kv.me, " : for too long there is no apply msg, reply fail: idx,seq=", idx, ctx.seq)
				ctx.replyFail()
				ctx.wakeUpCh <- nil
			}
			kv.onGoingCtxs = make(map[int]*onGoingCtx)
			kv.mu.Unlock()
			// }
		}

	}

}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.executedSequence = make(map[int]bool)
	kv.database = make(map[string]string)
	kv.onGoingCtxs = make(map[int]*onGoingCtx)
	kv.terminate = false

	// You may need initialization code here.

	go applyRoutine(kv)

	return kv
}
