import logging

from kvstore import DBMStore, InMemoryKVStore

LOG_LEVEL = logging.WARNING

KVSTORE_CLASS = InMemoryKVStore

"""
Possible abort modes.
"""
USER = 0
DEADLOCK = 1

"""
Part I: Implementing request handling methods for the transaction handler

The transaction handler has access to the following objects:

self._lock_table: the global lock table. More information in the README.

self._acquired_locks: a list of locks acquired by the transaction. Used to
release locks when the transaction commits or aborts. This list is initially
empty.

self._desired_lock: the lock that the transaction is waiting to acquire as well
as the operation to perform. This is initialized to None.

self._xid: this transaction's ID. You may assume each transaction is assigned a
unique transaction ID.

self._store: the in-memory key-value store. You may refer to kvstore.py for
methods supported by the store.

self._undo_log: a list of undo operations to be performed when the transaction
is aborted. The undo operation is a tuple of the form (@key, @value). This list
is initially empty.

You may assume that the key/value inputs to these methods are already type-
checked and are valid.
"""

class TransactionHandler:

    def __init__(self, lock_table, xid, store):
        # Values in lock table = 2 element list
            # 1st element = list of tuples (xid, lock)
            # 2nd element = FIFO queue 
        self._lock_table = lock_table
        self._acquired_locks = []
        self._desired_lock = None
        self._xid = xid
        self._store = store
        self._undo_log = []


    def perform_put(self, key, value):
        """
        Handles the PUT request. You should first implement the logic for
        acquiring the exclusive lock. If the transaction can successfully
        acquire the lock associated with the key, insert the key-value pair
        into the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.
        Hint: be aware that lock upgrade may happen.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted. See the code in abort()
        for the exact format.

        @param self: the transaction handler.
        @param key, value: the key-value pair to be inserted into the store.

        @return: if the transaction successfully acquires the lock and performs
        the insertion/update, returns 'Success'. If the transaction cannot
        acquire the lock, returns None, and saves the lock that the transaction
        is waiting to acquire in self._desired_lock.
        """

	    # Acquire exclusive lock 
        if not self.acquire_Xlock(key):
            self._desired_lock = (key, value, "X")
            return None
	 

	    # If got lock, can insert pair into store.
        old_value = self._store.get(key)
        self._store.put(key, value)

        # Update undo log
        self._undo_log.append((key, old_value))
        
        return 'Success'


    def acquire_Xlock(self, key):
        """
        Acquires exclusive lock, if possible. 

        @param self: the transaction handler
        @param key: key to acquire lock for
        @return: True if Xlock acquired. False if not. 
        """
        # If already have lock, done.
        own_lock = self.has_lock(key)
        if own_lock is not None and own_lock[1] == "X":
            return True

        # If no one locking it, or you're the only one locking it, get it. 
        if key not in self._lock_table:
            self._lock_table[key] = [[(self._xid, "X")], []]
            self._acquired_locks.append((key, "X"))
            return True
        if len(self._lock_table[key][0]) == 0:
            self._lock_table[key][0] = [(self._xid, "X")]
            self._acquired_locks.append((key, "X"))
            return True  

        # Only one locking
        lt_entry = self._lock_table[key][0]
        if len(lt_entry) == 1 and lt_entry[0][0] == self._xid:
            self._lock_table[key][0] = [(self._xid, "X")]
            self.upgrade_lock(key)
            return True 
	     
        curr_queue = self._lock_table[key][1]
        # If you want to upgrade, but other shares, cut queue.
        if len(lt_entry) > 1 and own_lock is not None and own_lock[1] == "S":
            self._lock_table[key][1] = [(self._xid, "X")] + curr_queue
            return False

        # Else, just get in the queue.
        self._lock_table[key][1] = curr_queue + [(self._xid, "X")]
        return False

    def has_lock(self, key):
        """
        Returns lock, if has it.

        @return: lock, if has it. None if does not. 
        """
        for i in range(len(self._acquired_locks)):
            if self._acquired_locks[i][0] == key:
                return self._acquired_locks[i]
        return None


    def upgrade_lock(self, key):
        """ 
        Updates self._acquired_locks, from "S" to "X"
        """
        for i in range(len(self._acquired_locks)):
            if self._acquired_locks[i][0] == key and self._acquired_locks[i][1] == "S":
                self._acquired_locks[i] = (key, "X")
                return 

    def perform_get(self, key):
        """ 
        Handles the GET request. You should first implement the logic for
        acquiring the shared lock. If the transaction can successfully acquire
        the lock associated with the key, read the value from the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.

        @param self: the transaction handler.
        @param key: the key to look up from the store.

        @return: if the transaction successfully acquires the lock and reads
        the value, returns the value. If the key does not exist, returns 'No
        such key'. If the transaction cannot acquire the lock, returns None,
        and saves the lock that the transaction is waiting to acquire in
        self._desired_lock.
        """
        
        # Acquire shared lock
            # If doesn't work, put in desired lock.
        if not self.acquire_Slock(key):
            self._desired_lock = (key, None, "S")
            return None 

        value = self._store.get(key)
        if value is None:
            return 'No such key'
        return value


    def acquire_Slock(self, key):
        """
        Acquires shared lock, if possible. 

        @return: True if Slock acquired. False if not. 

        """
        # If already have lock, done
        own_lock = self.has_lock(key)
        if own_lock is not None:
            return True

        # No downgrades allowed. 

        # If no one locking it, good.
        if key not in self._lock_table:
            self._lock_table[key] = [[(self._xid, "S")], []]
            self._acquired_locks.append((key, "S"))
            return True
        if len(self._lock_table[key][0]) == 0:
            self._lock_table[key][0] = [(self._xid, "S")]
            self._acquired_locks.append((key, "S"))
            return True

        # If someone has an exclusive lock on it, no go. 
        curr_queue = self._lock_table[key][1]
        if self.exists_Xlock(key):
            # Put self in queue
            curr_queue.append((self._xid, "S"))
            return False
        

        # Else, everyone on it has a shared lock.
        # Make sure not cutting queue though. 
        if len(curr_queue) == 0:
            self._lock_table[key][0].append((self._xid, "S"))
            self._acquired_locks.append((key, "S"))
            return True

        # Else, gotta get in the queue. 
        curr_queue.append((self._xid, "S"))
        return False 
        

    def exists_Xlock(self, key):
        lock_list = self._lock_table[key][0]
        for i in range(len(lock_list)):
            if lock_list[i][1] == "X":
                return True

        return False

    

    def release_and_grant_locks(self):
        """
        Releases all locks acquired by the transaction and grants them to the
        next transactions in the queue. This is a helper method that is called
        during transaction commits or aborts. 

        Hint: you can use self._acquired_locks to get a list of locks acquired
        by the transaction.
        Hint: be aware that lock upgrade may happen.

        @param self: the transaction handler.
        """

        for l in self._acquired_locks:
            key = l[0]
            lock_type = l[1]
                                
            # Gotta delete self from the lock table
            locks_for_key = self._lock_table[key][0]
            for i in range(len(locks_for_key)):
                if locks_for_key[i][0] == self._xid and locks_for_key[i][1] == lock_type:
                    self._lock_table[key][0].pop(i)
                    break


            # And grant to next in queue.
            self.grant_to_queue(key)

        self._acquired_locks = []


    def grant_to_queue(self, key):
        # If nothing in queue, done. 
        if len(self._lock_table[key][1]) == 0:
            return

        # Only grant to queue if existing locks are compatible.  
        try_more = 0
        queue = self._lock_table[key][1]
        first_in_queue = queue[0]
        if first_in_queue[1] == "S":
            # The first one, must be able to grant S. X would be gone.
            self.successful_queue_removal(first_in_queue[0], key, "S", 0)
            try_more = 1
        else:
            # Check if can get X.   
            if self.queue_acquire_Xlock(key, first_in_queue[0]):
                self.successful_queue_removal(first_in_queue[0], key, "X", 0)
        
                
        # If granted to an S lock, can maybe grant more too
        if try_more:
            i = 1
            while i < len(queue) and queue[i][1] == "S":
                if queue_acquire_Slock(key, queue[i][0]):
                    self.successful_queue_removal(queue[i][0], key, "S", i)
                else:
                    break
                i = i + 1
        

    def queue_acquire_Xlock(self, key, xid):
        # No one locking it
        if key not in self._lock_table or len(self._lock_table[key][0]) == 0:
            return True
        
        # Or you're the only one locking it
        lt_entry = self._lock_table[key][0]
        if len(lt_entry) == 1 and lt_entry[0][0] == xid:
            # Remove the S lock from lock_table. Acquired_locks done later.
            self._lock_table[key][0] = []
            return True

        # Can't have someone with an X lock, b/c releasing. 

        # If other shares, no.
        return False 
            

    def queue_acquire_Slock(self, key, xid):
        # If no one locking it, good.
        if key not in self._lock_table or len(self._lock_table[key][0]) == 0:
            return True

        # If someone has an exclusive lock, no. 
        if self.exists_Xlock(key):
            return False

        # Else, everyone has shared lock, join in. 
        return True 
         
            
    def successful_queue_removal(self, xid, key, lock_type, queue_pos):
        # Add to lock_table
        self._lock_table[key][0].append((xid, lock_type))
        # Fix queue
        queue = self._lock_table[key][1]
        queue.pop(queue_pos)
        
        
    def commit(self):
        """                     
        Commits the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.

        @return: returns 'Transaction Completed'
        """
        self.release_and_grant_locks()
        return 'Transaction Completed'

    def abort(self, mode):
        """
        Aborts the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.
        @param mode: mode can either be USER or DEADLOCK. If mode == USER, then
        it means that the abort is issued by the transaction itself (user
        abort). If mode == DEADLOCK, then it means that the transaction is
        aborted by the coordinator due to deadlock (deadlock abort).

        @return: if mode == USER, returns 'User Abort'. If mode == DEADLOCK,
        returns 'Deadlock Abort'.
        """
        while (len(self._undo_log) > 0):
            k,v = self._undo_log.pop()
            self._store.put(k, v)
        self.release_and_grant_locks()
        if (mode == USER):
            return 'User Abort'
        else:
            return 'Deadlock Abort'

    def check_lock(self):
        """
        If perform_get() or perform_put() returns None, then the transaction is
        waiting to acquire a lock. This method is called periodically to check
        if the lock has been granted due to commit or abort of other
        transactions. If so, then this method returns the string that would 
        have been returned by perform_get() or perform_put() if the method had
        not been blocked. Otherwise, this method returns None.

        As an example, suppose Joe is trying to perform 'GET a'. If Nisha has an
        exclusive lock on key 'a', then Joe's transaction is blocked, and
        perform_get() returns None. Joe's server handler starts calling
        check_lock(), which keeps returning None. While this is happening, Joe
        waits patiently for the server to return a response. Eventually, Nisha
        decides to commit his transaction, releasing his exclusive lock on 'a'.
        Now, when Joe's server handler calls check_lock(), the transaction
        checks to make sure that the lock has been acquired and returns the
        value of 'a'. The server handler then sends the value back to Joe.

        Hint: self._desired_lock contains the lock that the transaction is
        waiting to acquire.
        Hint: remember to update the self._acquired_locks list if the lock has
        been granted.
        Hint: if the transaction has been granted an exclusive lock due to lock
        upgrade, remember to clean up the self._acquired_locks list.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted.

        @param self: the transaction handler.

        @return: if the lock has been granted, then returns whatever would be
        returned by perform_get() and perform_put() when the transaction
        successfully acquired the lock. If the lock has not been granted,
        returns None.
        """

        key = self._desired_lock[0]
        value = self._desired_lock[1]
        lock_type = self._desired_lock[2]

        if self.granted_lock(key, lock_type):
            # Update self.acquired_locks
            self.update_acquired_locks(key, lock_type)

            # Remove desired_lock
            self._desired_lock = None

            if lock_type == "S":
                return self.perform_get(key)
            return self.perform_put(key, value)

        return None 


    def granted_lock(self, key, lock_type):
        lock_list = self._lock_table[key][0]
        for l in lock_list:
            if l[0] == self._xid and l[1] == lock_type:
                return True
        return False

    def update_acquired_locks(self, key, lock_type):
        # Upgrades only. Won't go from X to S. 
        own_lock = self.has_lock(key) # Old lock
        if lock_type == "X" and own_lock is not None and own_lock[1] == "S":
            self.upgrade_lock(key)
        else:
            self._acquired_locks.append((key, lock_type)) 


"""
Part II: Implement deadlock detection method for the transaction coordinator

The transaction coordinator has access to the following object:

self._lock_table: see description from Part I
"""

class TransactionCoordinator:

    def __init__(self, lock_table):
        self._lock_table = lock_table

    def detect_deadlocks(self):
        """
        Constructs a waits-for graph from the lock table, and runs a cycle
        detection algorithm to determine if a transaction needs to be aborted.
        You may choose which one transaction you plan to abort, as long as your
        choice is deterministic. For example, if transactions 1 and 2 form a
        cycle, you cannot return transaction 1 sometimes and transaction 2 the
        other times.

        This method is called periodically to check if any operations of any
        two transactions conflict. If this is true, the transactions are in
        deadlock - neither can proceed. If there are multiple cycles of
        deadlocked transactions, then this method will be called multiple
        times, with each call breaking one of the cycles, until it returns None
        to indicate that there are no more cycles. Afterward, the surviving
        transactions will continue to run as normal.

        Note: in this method, you only need to find and return the xid of a
        transaction that needs to be aborted. You do not have to perform the
        actual abort.

        @param self: the transaction coordinator.

        @return: If there are no cycles in the waits-for graph, returns None.
        Otherwise, returns the xid of a transaction in a cycle.
        """

        wf_graph = self.create_waitsfor_graph()    
        cycle_presence = detect_cycle(wf_graph)

        if cycle_presence[0]:
            return cycle_presence[1]
        return None 


    def create_waitsfor_graph(self):
        """ 
        Creates waits-for graph, using the self._lock_table. 

        @return: dictionary, that's like, the graph. 
            Key = xid; values = children xids. 
        """

        lock_table = self._lock_table

        # Graph points to things that are waiting for it, in the queue. 

        graph = {} 

        for key in lock_table.keys():
            curr_locks_list = lock_table[key][0]
            queue = lock_table[key][1]
            queue_exists = (len(queue) > 0)

            for curr_lock in curr_locks_list: 
                curr_lock_xid = curr_lock[0]
                if queue_exists and curr_lock[0] not in graph.keys():
                    # Add the key to the graph. 
                    graph[curr_lock_xid] = []

                for queue_lock in queue:
                    # Only draw edge if at least one is a write. 
                    if (curr_lock[1] == "X" or queue_lock[1] == "X"):
                        graph[curr_lock_xid].append(queue_lock[0])

                    # Make sure node exists in graph; is a transaction. 
                    if queue_lock[0] not in graph.keys():
                        graph[queue_lock[0]] = []

        return graph 



def detect_cycle(graph):
        # DFS. Algorithm reference, at bottom. 
        
        visited = {} # These nodes are already cleared. 
        stack = {} # Stuff in current path. 
        for node_xid in graph.keys():
            cycle_result = cycle_helper(node_xid, visited, stack, graph)
            if (cycle_result[0]):
                return cycle_result

        return (False, 0) 
        

def cycle_helper(curr_xid, visited, stack, graph): 
    if curr_xid not in visited.keys():
        visited[curr_xid] = True 
        # Also add to stack, because part of our path now
        stack[curr_xid] = True

        children = graph[curr_xid]
        for child_xid in children:
            if (child_xid not in visited.keys()) or (not visited[child_xid]):
                child_cycle_result = cycle_helper(child_xid, visited, stack, graph)
                if child_cycle_result[0]: 
                    return child_cycle_result
            # Else if no cycle from child, check if reaching child from here would be a cycle. 
            elif stack[child_xid]:
                return (True, child_xid)

    # Done checking out this node.
    stack[curr_xid] = False
    return (False, 0)



# Thanks to http://www.geeksforgeeks.org/detect-cycle-in-a-graph/, for help with cycle detection algorithm. 
