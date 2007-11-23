/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.transaction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The LockManager can lock resources for reading or writing. By doing this one
 * may achieve different transaction isolation levels. A resource can for now  
 * be any object (but null).
 * <p>
 * When acquiring a lock you have to release it. Failure to do so will result
 * in the resource being blocked to all other threads. Put all locks in a 
 * try - finally block.
 * <p>
 * Multiple locks on the same resource held by the same thread requires the
 * thread to invoke the release lock method multiple times. If a thread has
 * invoked <CODE>getReadLock</CODE> on the same resource x times in a row
 * it must invoke <CODE>releaseReadLock</CODE> x times to release all the locks.
 * <p>
 * LockManager just maps locks to resources and they do all the hard work 
 * together with a resource allocation graph.
 */
public class LockManager
{
	// private static final LockManager instance = new LockManager();
	
	private final Map<Object,RWLock> resourceLockMap = 
		new HashMap<Object,RWLock>();
	
	private RagManager ragManager = new RagManager();
	
	public LockManager() 
	{
	}
	
	/**
	 * Returns the single instance of this class.
	 *
	 * @return the lock manager
	 */
//	public static LockManager getManager()
//	{
//		return instance;
//	}
	
	/**
	 * Tries to acquire read lock on <CODE>resource</CODE> for the current 
	 * thread. If read lock can't be acquired the thread will wait for the 
	 * lock until it can acquire it. If waiting leads to dead lock a 
	 * {@link DeadlockDetectedException} will be thrown.
	 *
	 * @param resource The resource
	 * @throws DeadlockDetectedException If a deadlock is detected
	 * @throws IllegalResourceException
	 */
	public void getReadLock( Object resource ) 
		throws DeadlockDetectedException, IllegalResourceException
	{
		if ( resource == null )
		{
			throw new IllegalResourceException( "Null parameter" );
		}
		
		RWLock lock = null;
		synchronized ( resourceLockMap )
		{
			lock = resourceLockMap.get( resource );
			if ( lock == null )
			{
				lock = new RWLock( resource, ragManager );
				resourceLockMap.put( resource, lock );
			}
			lock.mark();
		}
		lock.acquireReadLock();
	}
	
	/**
	 * Tries to acquire write lock on <CODE>resource</CODE> for the current 
	 * thread. If write lock can't be acquired the thread will wait for the 
	 * lock until it can acquire it. If waiting leads to dead lock a 
	 * {@link DeadlockDetectedException} will be thrown.
	 *
	 * @param resource The resource
	 * @throws DeadlockDetectedException If a deadlock is detected
	 * @throws IllegalResourceException
	 */
	public void getWriteLock( Object resource )
		throws DeadlockDetectedException, IllegalResourceException	
	{
		if ( resource == null )
		{
			throw new IllegalResourceException( "Null parameter" );
		}

		RWLock lock = null;
		synchronized ( resourceLockMap )
		{
			lock = resourceLockMap.get( resource );
			if ( lock == null )
			{
				lock = new RWLock( resource, ragManager );
				resourceLockMap.put( resource, lock );
			}
			lock.mark();
		}
		lock.acquireWriteLock();
	}
	
	/**
	 * Releases a read lock held by the current thread on <CODE>resource</CODE>.
	 * If current thread don't have read lock a {@link LockNotFoundException}
	 * will be thrown.
	 *
	 * @param resource The resource
	 * @throws IllegalResourceException
	 * @throws LockNotFoundException
	 */
	public void releaseReadLock( Object resource ) 
		throws LockNotFoundException, IllegalResourceException
	{
		if ( resource == null )
		{
			throw new IllegalResourceException( "Null parameter" );
		}

		RWLock lock = null;
		synchronized ( resourceLockMap )
		{
			lock = resourceLockMap.get( resource );
			if ( lock == null )
			{
				throw new LockNotFoundException( "Lock not found for: " + 
					resource );
			}
			if ( !lock.isMarked() && lock.getReadCount() == 1 && 
				lock.getWriteCount() == 0 && 
				lock.getWaitingThreadsCount() == 0 )
			{
				resourceLockMap.remove( resource );
			}
			lock.releaseReadLock();
		}		
	}
	

	/**
	 * Releases a read lock held by the current thread on 
	 * <CODE>resource</CODE>. If current thread don't have read lock a 
	 * {@link LockNotFoundException} will be thrown.
	 *
	 * @param resource The resource
	 * @throws IllegalResourceException
	 * @throws LockNotFoundException
	 */
	public void releaseWriteLock( Object resource ) 
		throws LockNotFoundException, IllegalResourceException
	{
		if ( resource == null )
		{
			throw new IllegalResourceException( "Null parameter" );
		}

		RWLock lock = null;
		synchronized ( resourceLockMap )
		{
			lock = resourceLockMap.get( resource );
			if ( lock == null )
			{
				throw new LockNotFoundException( "Lock not found for: " +
					resource );
			}
			if ( !lock.isMarked() && lock.getReadCount() == 0 && 
				lock.getWriteCount() == 1 && 
				lock.getWaitingThreadsCount() == 0 )
			{
				resourceLockMap.remove( resource );
			}
			lock.releaseWriteLock();
		}		
		
	}
	
	/**
	 * Utility method for debugging. Dumps info to console of threads having
	 * locks on resources.
	 * 
	 * @param resource
	 */
	public void dumpLocksOnResource( Object resource )
	{
		RWLock lock = null;
		synchronized ( resourceLockMap )
		{
			if ( !resourceLockMap.containsKey( resource ) )
			{
				System.out.println( "No locks on " + resource );
				return;
			}
			lock = resourceLockMap.get( resource );
		}
		lock.dumpStack();
	}
	
	/**
	 * Utility method for debugging. Dumps the resource allocation graph
	 * to console.
	 */
	public void dumpRagStack()
	{
		ragManager.dumpStack();
	}
	
	/**
	 * Utility method for debuggin. Dumps info about each lock to console.
	 */
	public void dumpAllLocks()
	{
		synchronized ( resourceLockMap )
		{
			Iterator<RWLock> itr = resourceLockMap.values().iterator();
			int emptyLockCount = 0;
			while ( itr.hasNext() )
			{
				RWLock lock = itr.next();
				if ( lock.getWriteCount() > 0 || lock.getReadCount() > 0 )
				{
					lock.dumpStack();
				}
				else
				{
					if ( lock.getWaitingThreadsCount() > 0 ) 
					{
						lock.dumpStack();
					}
					emptyLockCount++;
				}
			}
			if ( emptyLockCount > 0 )
			{
				System.out.println( "There are " + emptyLockCount + 
					" empty locks" );
			}
			else
			{
				System.out.println( "There are no empty locks" );
			}
		}
	}
}
