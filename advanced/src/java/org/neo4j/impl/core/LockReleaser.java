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
package org.neo4j.impl.core;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import javax.transaction.InvalidTransactionException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.transaction.NotInTransactionException;
import org.neo4j.impl.transaction.TransactionIsolationLevel;
import org.neo4j.impl.util.ArrayMap;

/**
 * Manages commands and locks for each transaction. The public methods 
 * <CODE>releaseCommands</CODE> and <CODE>undoAndReleaseCommands</CODE> should
 * only be invoked right after a transaction commit or rollback. Depending
 * on {@link TransactionIsolationLevel} locks will be added here to be 
 * released upon commit/rollback. 
 */
public class LockReleaser
{
	private static Logger log = Logger.getLogger( 
		LockReleaser.class.getName() );
	
	private final ArrayMap<Thread,List<LockElement>> lockMap =  
			new ArrayMap<Thread,List<LockElement>>( 5, true, true );

	private final LockManager lockManager;
	private final TransactionManager transactionManager;
	
	public LockReleaser( LockManager lockManager, 
		TransactionManager transactionManager )
	{
		this.lockManager = lockManager;
		this.transactionManager = transactionManager;
	}
	
	private static class LockElement
	{
		Object resource;
		LockType lockType;
		
		LockElement( Object resource, LockType type )
		{
			this.resource = resource;
			this.lockType = type;
		}
	}
	
	/**
	 * Depending on transaction isolation level a lock may be released 
	 * as soon as possible or it may be held throughout the whole transaction.
	 * Invoking this method will trigger a release lock of {@link LockType} 
	 * <CODE>type</CODE> on the <CODE>resource</CODE> when the transaction 
	 * commits or rollbacks.
	 * 
	 * @param resource the resource on which the lock is taken
	 * @param type type of lock (READ or WRITE)
	 * @throws NotInTransactionException
	 */
	public void addLockToTransaction( Object resource, LockType type ) 
		throws NotInTransactionException
	{
		Thread currentThread = Thread.currentThread();
        List<LockElement> lockElements = lockMap.get( currentThread );
		if ( lockElements != null )
		{
			lockElements.add( new LockElement( resource, type ) );
		}
		else
		{
			Transaction tx = null;
			try
			{
				tx = transactionManager.getTransaction();
				if ( tx == null )
				{
					// no transaction we release lock right away
					if ( type == LockType.WRITE )
					{
						lockManager.releaseWriteLock( resource );
					}
					else if ( type == LockType.READ )
					{
						lockManager.releaseReadLock( resource );
					}
					throw new NotInTransactionException();
				}
				tx.registerSynchronization( new TxCommitHook( this ) );
			}
			catch ( javax.transaction.SystemException e )
			{
				throw new NotInTransactionException( e );
			}
			catch ( Exception e )
			{
				throw new NotInTransactionException( e );
			}
			lockElements = new ArrayList<LockElement>();
			lockMap.put( currentThread, lockElements );
			lockElements.add( new LockElement( resource, type ) );
		}
	}

	/**
	 * Releases all commands that participated in the successfully committed
	 * transaction.
	 *
	 * @throws InvalidTransactionException if this method is invoked when 
	 * transaction state is invalid.
	 */
	public void releaseLocks()
	{
		Thread currentThread = Thread.currentThread();
		List<LockElement> lockElements = lockMap.remove( currentThread );
		if ( lockElements != null )
		{
			for ( LockElement lockElement : lockElements )
			{
				try
				{
					if ( lockElement.lockType == LockType.READ )
					{
						lockManager.releaseReadLock( lockElement.resource );
					}
					else if ( lockElement.lockType == LockType.WRITE )
					{
						lockManager.releaseWriteLock( lockElement.resource );
					}
				}
				catch ( Exception e )
				{
					e.printStackTrace();
					log.severe( "Unable to release lock[" + 
						lockElement.lockType + "] on resource[" + 
						lockElement.resource + "]" );
				}
			}
		}
	}
	
	public synchronized void dumpLocks()
	{
		System.out.print( "Locks held: " );
		java.util.Iterator<?> itr = lockMap.keySet().iterator();
		if ( !itr.hasNext() )
		{
			System.out.println( "NONE" );
		}
		else
		{
			System.out.println();
		}
		while ( itr.hasNext() )
		{
			Thread thread = (Thread) itr.next();
			System.out.println( "" + thread + "->" +  
				lockMap.get( thread ).size() );
		}
	}
}		