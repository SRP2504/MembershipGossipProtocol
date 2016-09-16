/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        int id = 0;
        short port = 0;
        memcpy (&id, (char *)memberNode->addr.addr, sizeof(int));
        memcpy (&port, (char *)(memberNode->addr.addr) + sizeof(int), sizeof(short));
        MemberListEntry entry (id, port, memberNode->heartbeat, par->globaltime);
        memberNode->memberList.push_back (entry);
        log->logNodeAdd (&memberNode->addr, new Address (getAddressString(id, port)));
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MessageHdr *hdr = (MessageHdr *)(data);

    switch (hdr->msgType) {
        case JOINREQ: {
            char addr[sizeof(memberNode->addr.addr)];
            memcpy (addr, (char *)(hdr+1), sizeof(memberNode->addr.addr));
            int id = 0;
            short port = 0;
            memcpy (&id, addr, sizeof(int));
            memcpy (&port, addr+sizeof(int), sizeof(short));

            long heartbeat;
            memcpy (&heartbeat, (char *)(hdr+1) + 1 + sizeof(memberNode->addr.addr), sizeof(long));
            cout << "JOINREQ from " << id << ":" << port << endl;

            if (rand()%2 == 0) {
                MemberListEntry entry (id, port, heartbeat, par->globaltime);
                memberNode->memberList.push_back (entry);
                log->logNodeAdd (&memberNode->addr, new Address (getAddressString(id, port)));
            }

            MessageHdr *msg;
            int listSize = memberNode->memberList.size ();
            size_t msgSize = sizeof(MessageHdr) + sizeof(int) + (listSize * sizeof(MemberListEntry));
            msg = (MessageHdr *) malloc(msgSize * sizeof(char));
            msg->msgType = JOINREP;

            memcpy ((char *)(msg+1), &listSize, sizeof(int));

            for (int i = 0; i < listSize; i++) {
                memcpy ((char *)(msg+1) + sizeof(int) + (i * sizeof(MemberListEntry)), 
                        &memberNode->memberList[i],
                        sizeof(MemberListEntry));
            }
            emulNet->ENsend(&memberNode->addr, new Address(to_string(id)+":"+to_string(port)), (char *)msg, msgSize);
            break; 
        }
        case JOINREP: {
            cout << memberNode->addr.getAddress() << " received Msg : " << "JOINREP ";
            int listSize;
            memcpy (&listSize, (char *)(hdr + 1), sizeof(int));
            cout << listSize << endl;
            memberNode->memberList.resize (listSize);
            for (int i = 0; i < listSize; i++) {
                memcpy (&memberNode->memberList[i],
                        (char *)(hdr+1) + sizeof(int) + (i * sizeof(MemberListEntry)),
                        sizeof(MemberListEntry));
                log->logNodeAdd (&memberNode->addr, new Address(getAddressString(memberNode->memberList[i].getid(),
                                                                                memberNode->memberList[i].getport())));
            }
            memberNode->inGroup = true;
            break;
        }
        case HEARTBEAT: {
            if (memberNode->inGroup) {
                int lsize;
                memcpy (&lsize, (char *)(hdr+1), sizeof(int));
                vector<MemberListEntry> entries (lsize);
                for (int i = 0; i < lsize; i++) {
                    memcpy (&entries[i],
                            (char *)(hdr+1) + sizeof(int) + (i*sizeof(MemberListEntry)), 
                            sizeof(MemberListEntry));    
                }
                int csize = memberNode->memberList.size();
                for (int i = 0; i < lsize; i++) {
                    int id = entries[i].getid();
                    short port = entries[i].getport(); 
                    int j = 0;;
                    for (j = 0; j < csize; j++) {
                        if (memberNode->memberList[j].getid() == id && 
                            memberNode->memberList[j].getport() == port) {
                            if (memberNode->memberList[j].getheartbeat() < entries[i].getheartbeat()) {
                                memberNode->memberList[j].heartbeat = entries[i].getheartbeat();
                                memberNode->memberList[j].timestamp = par->globaltime;
                            }
                            break;
                        }
                    }
                    if (j == csize) {
                        MemberListEntry entry(id, port, entries[i].getheartbeat(), par->globaltime);
                        memberNode->memberList.push_back (entry);
                        log->logNodeAdd(&memberNode->addr, new Address(getAddressString(id, port)));
                    }
                }
            }
            break;
        }
        default:
            break;
    }
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    memberNode->heartbeat++;
    updateSelfHeartbeat ();
    removeFailedNodes ();
    int listSize = memberNode->memberList.size ();
    if (listSize > 0) {
        int index = rand()%listSize;
        sendHeartbeat (index);
        if (listSize > 1) {
            int index = rand()%listSize;
            sendHeartbeat (index);
        }
    }
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

string MP1Node::getAddressString (int id, short port) {
    return to_string(id) + ":" + to_string(port);
}

void MP1Node::updateSelfHeartbeat () {
    int listSize = memberNode->memberList.size ();
    int i = 0;
    for (i = 0; i < listSize; i++) {
        string address = getAddressString (memberNode->memberList[i].getid(), memberNode->memberList[i].getport());
        if (memberNode->addr.getAddress().compare(address) == 0) {
            memberNode->memberList[i].heartbeat = memberNode->heartbeat;
            memberNode->memberList[i].timestamp = par->globaltime;
            break;
        }
    }
    if (i == listSize) {
        int id = 0;
        short port = 0;
        memcpy (&id, memberNode->addr.addr, sizeof(int));
        memcpy (&port, (char *)(memberNode->addr.addr) + sizeof(int), sizeof(short));
        MemberListEntry entry (id, port, memberNode->heartbeat, par->globaltime);
        memberNode->memberList.push_back (entry);
        log->logNodeAdd (&memberNode->addr, new Address(getAddressString(id, port)));
    }
}

void MP1Node::removeFailedNodes () {
    vector<MemberListEntry>::iterator it = memberNode->memberList.begin ();
    for (; it != memberNode->memberList.end(); ) {
        if ((par->globaltime - it->timestamp) >= TFAIL + TREMOVE) {
            log->logNodeRemove (&memberNode->addr, new Address(getAddressString(it->id, it->port)));
            it = memberNode->memberList.erase (it);
        } else {
            it++;
        }
    }
}

void MP1Node::sendHeartbeat (int index) {
    MessageHdr *msg;
    int listSize = memberNode->memberList.size ();
    int cnt = 0;
    for (int i = 0; i < listSize; i++) {
        if ((par->globaltime - memberNode->memberList[i].timestamp) > TFAIL) {
            continue;
        }
        cnt++;
    }
    size_t msgSize = sizeof(MessageHdr) + sizeof(int) + (cnt * sizeof(MemberListEntry));
    msg = (MessageHdr *) malloc(msgSize * sizeof(char));
    msg->msgType = HEARTBEAT;

    memcpy ((char *)(msg+1), &cnt, sizeof(int));

    string address = getAddressString(memberNode->memberList[index].getid(), memberNode->memberList[index].getport());
    int j = 0;
    for (int i = 0; i < listSize; i++) {
        if ((par->globaltime - memberNode->memberList[i].timestamp) <= TFAIL) {
            memcpy ((char *)(msg+1) + sizeof(int) + (j * sizeof(MemberListEntry)), 
                    &memberNode->memberList[i],
                    sizeof(MemberListEntry));
            j++;
        }
    }
    emulNet->ENsend(&memberNode->addr, new Address(address), (char *)msg, msgSize);
}
