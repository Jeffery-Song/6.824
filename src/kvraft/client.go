package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "fmt"

var seqLock sync.Mutex
var nextSequence int = 0

func genSeq() int {
	seqLock.Lock()
	seq := nextSequence
	nextSequence++
	seqLock.Unlock()
	return seq
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastRemLeader int
	// nextSequence int
	mu      sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastRemLeader = 0
	// ck.nextSequence = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	seq := genSeq()
	dest := ck.lastRemLeader
	for {
		args := GetArgs{}
		args.Sequence = seq
		args.Key = key
		reply := GetReply{}
		myDebug("client sending get to ", dest, ", key=\""+fmt.Sprint(key)+"\", seq=" + fmt.Sprint(seq))

		ok := ck.servers[dest].Call("KVServer.Get", &args, &reply)
		if !ok {
			myDebug("client thinks rpc failed, retry, seq=" + fmt.Sprint(seq))
			dest = (dest + 1) % len(ck.servers)
			continue
		}
		
		switch reply.Err {
		case NotLeader:
			myDebug("server says not leader, retry, seq=" + fmt.Sprint(seq))
			dest = (dest + 1) % len(ck.servers)
			continue
		case WrongOperation:
			// TODO: sometiong wrong happend
			return ""
		case CommitFail:
			myDebug("server says commit fail, retry, seq=" + fmt.Sprint(seq))
			dest = (dest + 1) % len(ck.servers)
			continue
		case "":
			ck.mu.Lock()
			ck.lastRemLeader = dest
			ck.mu.Unlock()
			return reply.Value
		default :
			myDebug("Unknow Err str: \""+reply.Err+"\"")
		}
	}
	// return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	seq := genSeq()
	dest := ck.lastRemLeader
	for {
		args := PutAppendArgs{}
		args.Sequence = seq
		args.Key = key
		args.Value = value
		args.Op = op
		reply := GetReply{}
		myDebug("client sending putappend to ", dest, ", op=" + op + ", key=\""+key+"\", val=\"" + value + "\", seq=" + fmt.Sprint(seq))
		ok := ck.servers[dest].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			myDebug("client thinks rpc failed, retry, seq=" + fmt.Sprint(seq))
			dest = (dest + 1) % len(ck.servers)
			continue
		}
		
		switch reply.Err {
		case NotLeader:
			myDebug("server says not leader, retry, seq=" + fmt.Sprint(seq))
			dest = (dest + 1) % len(ck.servers)
			continue
		case WrongOperation:
			// TODO: sometiong wrong happend
			return
		case CommitFail:
			myDebug("server says commit fail, retry, seq=" + fmt.Sprint(seq))
			dest = (dest + 1) % len(ck.servers)
			continue
		case "":
			ck.mu.Lock()
			ck.lastRemLeader = dest
			ck.mu.Unlock()
			return
		default :
			myDebug("Unknow Err str: \""+reply.Err+"\"")
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
