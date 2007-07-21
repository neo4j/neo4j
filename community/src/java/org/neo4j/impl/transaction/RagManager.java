package org.neo4j.impl.transaction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;
import java.util.Stack;

/**
 * The Resource Allocation Graph manager is used for deadlock detection. It
 * keeps track of all locked resources and threads waiting for resources. 
 * When a {@link RWLock} cannot give the lock to a thread the thread has
 * to wait and that may lead to a deadlock. So before the thread is put into
 * wait mode the {@link RagManager#checkWaitOn} method is invoked to check
 * if a wait of this thread will lead to a deadlock. 
 * <p>
 * The <CODE>checkWaitOn</CODE> throws a {@link DeadlockDetectedException} if 
 * a deadlock would occur when the thread would wait for the resouce. 
 * That will guarantee that a deadlock never occurs on a RWLock basis.
 * <p>
 * Think of the resource allocation graph as a node space. We have two node 
 * types, resource nodes (R) and thread/process nodes (T). When a thread 
 * acquires lock on some resource a relationship is added from the resource
 * to the thread (R->T) and when a thread waits for a resource a relationship 
 * is added from the thread to the resource (T->R). The only thing we need to 
 * do to see if a deadlock occurs when some thread waits for a resource 
 * is to traverse node nodespace starting on the resource and see if we can get 
 * back to the thread ( T1 wanna wait on R1 and R1->T2->R2->T3->R8->T1 <==> 
 * deadlock!).
 */
class RagManager
{
	// if a runtime exception is thrown from any method it means that the 
	// RWLock class hasn't keept the contract to the RagManager
	// The contract is:
	// o When a thread gets a lock on a resource and both the readCount and 
	//   writeCount for that thread on the resource was 0  
	//   RagManager.lockAcquired( resource ) must be invoked
	// o When a thread releases a lock on a resource and both the readCount and 
	//   writeCount for that thread on the resource goes down to zero 
	//   RagManager.lockReleased( resource ) must be invoked
	// o After invoke to the checkWaitOn( resource ) method that didn't result 
	//   in a DeadlockDetectedException the thread must wait
	// o When the thread wakes up from waiting on a resource the 
	//   stopWaitOn( resource ) method must be invoked
	
	private static RagManager instance = new RagManager();
	
	private Map<Object,List<Thread>> resourceMap = 
		new HashMap<Object,List<Thread>>();
	
	// key = Thread
	// value = resource that the Thread is waiting for
	private Map<Thread,Object> waitingThreadMap = 
		new HashMap<Thread,Object>();
	
	private RagManager() {}
	
	static RagManager getManager()
	{
		return instance;
	}
	
	synchronized void lockAcquired( Object resource )
	{
		Thread currentThread = Thread.currentThread();
		List<Thread> lockingThreadList = resourceMap.get( resource );
		if ( lockingThreadList != null )
		{
			assert !lockingThreadList.contains( currentThread );
			lockingThreadList.add( currentThread );
		}
		else
		{
			lockingThreadList = new LinkedList<Thread>();
			lockingThreadList.add( currentThread );
			resourceMap.put( resource, lockingThreadList );
		}
	}
	
	synchronized void lockReleased( Object resource )
	{
		List<Thread> lockingThreadList = resourceMap.get( resource );
		if ( lockingThreadList == null )
		{
			throw new RuntimeException( "Resource not found in resource map" );
		}
		
		Thread currentThread = Thread.currentThread();
		if ( !lockingThreadList.remove( currentThread ) )
		{
			throw new RuntimeException( "Thread not found in locking thread list" );
		}
		if ( lockingThreadList.size() == 0 )
		{
			resourceMap.remove( resource );
		}
	}

	synchronized void stopWaitOn( Object resource )
	{
		Thread currentThread = Thread.currentThread();
		if ( waitingThreadMap.remove( currentThread ) == null )
		{
			throw new RuntimeException( "Thread not waiting on resource" );
		}	
	}

	// after invoke the thread must wait on the resource
	synchronized void checkWaitOn( Object resource ) 
		throws DeadlockDetectedException
	{
		List<Thread> lockingThreadList = resourceMap.get( resource );
		if ( lockingThreadList == null )
		{
			throw new RuntimeException( "Illegal resource, not found in map" );
		}
		
		Thread waitingThread = Thread.currentThread();
		if ( waitingThreadMap.containsKey( waitingThread ) )
		{
			throw new RuntimeException( "Thread already waiting for resource" );
		}

		Iterator<Thread> itr = lockingThreadList.iterator();
		List<Thread> checkedThreads = new LinkedList<Thread>();
		Stack<Object> graphStack = new Stack<Object>();
		// has resource,Thread interleaved
		graphStack.push( resource );
		while ( itr.hasNext() )
		{
			Thread lockingThread = itr.next();
			// the if statement bellow is valid because:
			// t1 -> r1 -> t1 (can happend with RW locks) is ok but,
			// t1 -> r1 -> t1&t2 where t2 -> r1 is a deadlock
			// think like this, we have two threads and one resource
			// o t1 takes read lock on r1
			// o t2 takes read lock on r1
			// o t1 wanna take write lock on r1 but has to wait for t2
			//   to release the read lock ( t1->r1->(t1&t2), ok not deadlock yet
			// o t2 wanna take write lock on r1 but has to wait for t1
			//   to release read lock.... 
			//   DEADLOCK t1->r1->(t1&t2) and t2->r1->(t1&t2) ===>
			//   t1->r1->t2->r1->t1, t2->r1->t1->r1->t2 etc...
			// to allow the first three steps above we check if lockingThread ==
			// waitingThread on first level.
			// because of this special case we have to keep track on the 
			// already "checked" threads since it is (now) legal for one type of 
			// circular	reference to exist (t1->r1->t1) otherwise we may traverse
			// t1->r1->t2->r1->t2->r1->t2... until SOE
			// ... KISS to you too
			if ( lockingThread != waitingThread )
			{
				graphStack.push( lockingThread );
				checkWaitOnRecursive( lockingThread, waitingThread, 
					checkedThreads, graphStack );
				graphStack.pop();
			}
		}
		
		// ok no deadlock, we can wait on resource
		waitingThreadMap.put( Thread.currentThread(), resource );
	}
	
	private synchronized void checkWaitOnRecursive( Thread lockingThread, 
		Thread waitingThread, List<Thread> checkedThreads, 
		Stack<Object> graphStack ) throws DeadlockDetectedException
	{
		if ( lockingThread == waitingThread )
		{
			StringBuffer circle = null;
			Object resource = null;
			do
			{
				lockingThread = (Thread) graphStack.pop();
				resource = graphStack.pop();
				if ( circle == null )
				{
					circle = new StringBuffer();
					circle.append( lockingThread + " <- " + resource );
				}
				else
				{
					circle.append( " <- " + lockingThread + " <- " + resource );
				}
			} while ( !graphStack.isEmpty() );
			throw new DeadlockDetectedException( waitingThread + 
				" can't wait on resource " + resource + 
				" since => " + circle );
		}
		checkedThreads.add( lockingThread );
		Object resource = waitingThreadMap.get( lockingThread );
		if ( resource != null )
		{
			graphStack.push( resource );
			// if the resource doesn't exist in resorceMap that means all the
			// locks on the resource has been released
			// it is possible when this thread was in RWLock.acquire and 
			// saw it hade to wait for the lock the scheduler changes to some 
			// other thread that will relsease the locks on the resource and 
			// remove it from the map
			// this is ok since current thread or any other thread will wake
			// in the synchronized block and will be forced to do the deadlock 
			// check once more if lock cannot be acquired
			List<Thread> lockingThreadList = resourceMap.get( resource );
			if ( lockingThreadList != null )
			{
				Iterator<Thread> itr = lockingThreadList.iterator();
				while ( itr.hasNext() )
				{
					lockingThread = itr.next();
					// so we don't 
					if ( !checkedThreads.contains( lockingThread ) )
					{
						graphStack.push( lockingThread );
						checkWaitOnRecursive( lockingThread, waitingThread, 
							checkedThreads, graphStack );
						graphStack.pop();
					}
				}
			}
			graphStack.pop();
		}
	}
	
	synchronized void dumpStack()
	{
		System.out.print( "Waiting list: " );
		Iterator itr = waitingThreadMap.keySet().iterator();
		if ( !itr.hasNext() )
		{
			System.out.println( "No threads waiting on resources" );
		}
		else
		{
			System.out.println();
		}
		while ( itr.hasNext() )
		{
			Thread thread = (Thread) itr.next();
			System.out.println( "" + thread + "->" + 
				waitingThreadMap.get( thread ) );
		}
		System.out.print( "Resource lock list: " );
		itr = resourceMap.keySet().iterator();
		if ( !itr.hasNext() )
		{
			System.out.println( "No locked resources found" );
		}
		else
		{
			System.out.println();
		}
		while ( itr.hasNext() )
		{
			Object resource = itr.next();
			System.out.print( "" + resource + "->" );
			java.util.Iterator itr2 = 
				((List) resourceMap.get( resource )).iterator();
			if ( !itr2.hasNext() )
			{
				System.out.println( " Error empty list found" );
			}
			while ( itr2.hasNext() )
			{
				System.out.print( "" + itr2.next() );
				if ( itr2.hasNext() )
				{
					System.out.print( "," );
				}
				else
				{
					System.out.println();
				}
			}
		}
	}
}