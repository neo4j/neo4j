package org.neo4j.impl.command;

import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.transaction.NotInTransactionException;
import org.neo4j.impl.transaction.TransactionFactory;
import org.neo4j.impl.transaction.TransactionIsolationLevel;

import java.util.Map;
import java.util.HashMap;
import java.util.Stack;
import java.util.logging.Logger;

import javax.transaction.Transaction;
import javax.transaction.InvalidTransactionException;
import javax.transaction.Synchronization;

/**
 * Manages commands and locks for each transaction. The public methods 
 * <CODE>releaseCommands</CODE> and <CODE>undoAndReleaseCommands</CODE> should
 * only be invoked right after a transaction commit or rollback. Depending
 * on {@link TransactionIsolationLevel} locks will be added here to be 
 * released upon commit/rollback. 
 */
public class CommandManager
{
	private static Logger log = Logger.getLogger( 
		CommandManager.class.getName() );
	
	private static final TransactionIsolationLevel DEFAULT_ISOLATION_LEVEL = 
		TransactionIsolationLevel.READ_COMMITTED;
	
	private static CommandManager instance = new CommandManager();
	
	private Map<Thread,CommandStackElement> commandStack = 
		java.util.Collections.synchronizedMap( 
			new HashMap<Thread,CommandStackElement>() );

	private Synchronization txCommitHook = new TxCommitHook();
	
	private CommandManager()
	{
	}
	
	/**
	 * Returns the single instance of this class.
	 * 
	 * @return The command manager
	 */
	public static CommandManager getManager()
	{
		return instance;
	}
	
	private static class CommandStackElement
	{
		TransactionIsolationLevel isolationLevel;
		Stack<Command> commands;
		Stack<LockElement> locks;
		
		CommandStackElement( TransactionIsolationLevel level, 
			Stack<Command> stack )
		{
			this.isolationLevel = level;
			this.commands = stack;
		}
		
		public String toString()
		{
			int commandCount = 0;
			if ( commands != null )
			{
				commandCount = commands.size();
			}
			int lockCount = 0;
			if ( locks != null )
			{
				lockCount = locks.size();
			}
			return "CommandCount[" + commandCount + "], LockCount[" + 
				lockCount + "]";
		}
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
	
	// no need to synchronize for now, we're using thread as key
	void addCommandToTransaction( Command command ) 
		throws NotInTransactionException
	{
		Transaction tx = null;
		try
		{
			tx = TransactionFactory.getTransactionManager().getTransaction();
			if ( tx == null )
			{
				throw new NotInTransactionException();
			}
		}
		catch ( javax.transaction.SystemException e )
		{
			throw new NotInTransactionException( e );
		}
		Thread currentThread = Thread.currentThread();
		CommandStackElement cse = commandStack.get( currentThread );
		if ( cse != null )
		{
			cse.commands.push( command );
		}
		else
		{
			try
			{
				tx.registerSynchronization( txCommitHook );
			}
			catch ( Exception e )
			{
				throw new NotInTransactionException( e );
			}
			cse = new CommandStackElement( DEFAULT_ISOLATION_LEVEL, 
				new Stack<Command>() );
			cse.commands.push( command );
			commandStack.put( currentThread, cse );
		}
	}
	
	boolean txCommitHookRegistered()
	{
		return commandStack.containsKey( Thread.currentThread() );
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
		Transaction tx = null;
		try
		{
			tx = TransactionFactory.getTransactionManager().getTransaction();
			if ( tx == null )
			{
				throw new NotInTransactionException();
			}
		}
		catch ( javax.transaction.SystemException e )
		{
			throw new NotInTransactionException( e );
		}
		Thread currentThread = Thread.currentThread();
                CommandStackElement cse = commandStack.get( currentThread );
		if ( cse != null )
		{
			if ( cse.locks == null )
			{
				cse.locks = new Stack<LockElement>();
			}
			cse.locks.push( new LockElement( resource, type ) );
		}
		else
		{
			try
			{
				tx.registerSynchronization( txCommitHook );
			}
			catch ( Exception e )
			{
				throw new NotInTransactionException( e );
			}
			cse = new CommandStackElement( DEFAULT_ISOLATION_LEVEL, 
				new Stack<Command>() );
			commandStack.put( currentThread, cse );
			if ( cse.locks == null )
			{
				cse.locks = new Stack<LockElement>();
			}
			cse.locks.push( new LockElement( resource, type ) );
		}
	}

	/**
	 * Changes the transaction isolation level for this transaction. This 
	 * should be done right after <CODE>UserTransaction.begin()</CODE>. Trying 
	 * to change isolation level after commands or locks has been added to the 
	 * transaction will result in a {@link InvalidTransactionException} beeing 
	 * thrown. 
	 * 
	 * @param level new transaction isolation level
	 * @throws InvalidTransactionException if unable to change isolation 
	 * level for this transaction
	 * @throws NotInTransactionException
	 */
	public void setTransactionIsolationLevel( TransactionIsolationLevel level )
		throws InvalidTransactionException, NotInTransactionException
	{
		Transaction tx = null;
		try
		{
			tx = TransactionFactory.getTransactionManager().getTransaction();
			if ( tx == null )
			{
				throw new NotInTransactionException();
			}
		}
		catch ( javax.transaction.SystemException e )
		{
			throw new NotInTransactionException( e );
		}

		Thread currentThread = Thread.currentThread();
		if ( commandStack.containsKey( currentThread ) )
		{
			throw new InvalidTransactionException( 
				"Transaction already in use." );
		}
		else
		{
			try
			{
				tx.registerSynchronization( txCommitHook );
			}
			catch ( Exception e )
			{
				throw new NotInTransactionException( e );
			}
			commandStack.put( currentThread, 
				new CommandStackElement( level, new Stack<Command>() ) );
		}
	}
	
	/**
	 * Returns the transaction isolation level for the current transaction. 
	 *
	 * @return the transaction isolation level
	 * @throws NotInTransactionException
	 */
	public TransactionIsolationLevel getTransactionIsolationLevel()
		throws NotInTransactionException
	{
		try
		{
			if ( TransactionFactory.getTransactionManager().getTransaction() ==
						null )
			{
				throw new NotInTransactionException();
			}
		}
		catch ( javax.transaction.SystemException e )
		{
			throw new NotInTransactionException( e );
		}
		
		Thread currentThread = Thread.currentThread();
		CommandStackElement cse = commandStack.get( currentThread );
		if ( cse != null )
		{
			return cse.isolationLevel;
		}
		else
		{
			return DEFAULT_ISOLATION_LEVEL;
		}
	}
	
	/**
	 * Releases all commands that participated in the successfully commited
	 * transaction.
	 *
	 * @throws InvalidTransactionException if this method is invoked when 
	 * transaction state is invalid.
	 */
	public void releaseCommands()
	{
		Thread currentThread = Thread.currentThread();
		CommandStackElement cse = commandStack.remove( currentThread );
		if ( cse != null )
		{
			Stack<LockElement> lStack = cse.locks;
			while ( lStack != null && !lStack.isEmpty() )
			{
				LockElement lockElement = lStack.pop();
				try
				{
					if ( lockElement.lockType == LockType.READ )
					{
						LockManager.getManager().releaseReadLock( 
							lockElement.resource );
					}
					else if ( lockElement.lockType == LockType.WRITE )
					{
						LockManager.getManager().releaseWriteLock( 
							lockElement.resource );
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
			
			if ( cse.commands != null )
			{
				cse.commands.empty();
			}
		}
	}
	
	/**
	 * All commands that participated in the rollbacked transaction will be 
	 * be undone and released back to command pool.
	 * <p>
	 * If any undo operation fails an {@link UndoFailedException} is thrown
	 * after all commands in the transaction has been done.
	 *
	 * @throws InvalidTransactionException if this method is invoked when
	 * transactionstate is invalid
	 */
	public void undoAndReleaseCommands()
	{
		Thread currentThread = Thread.currentThread();
		if ( commandStack.containsKey( currentThread ) )
		{
			UndoFailedException ufe = null;
			int undoFailedCount = 0;
			CommandStackElement cse = commandStack.remove( currentThread );
			Stack<Command> cStack = cse.commands;
			while ( !cStack.isEmpty() )
			{
				Command command = cStack.pop();
				try
				{
					command.undo();
				}
				catch ( UndoFailedException e )
				{
					undoFailedCount++;
					if ( ufe == null )
					{
						ufe = e;
					}
				}
			}

			Stack<LockElement> lStack = cse.locks;
			while ( lStack != null && !lStack.isEmpty() )
			{
				LockElement lockElement = lStack.pop();
				try
				{
					if ( lockElement.lockType == LockType.READ )
					{
						LockManager.getManager().releaseReadLock( 
							lockElement.resource );
					}
					else if ( lockElement.lockType == LockType.WRITE )
					{
						LockManager.getManager().releaseWriteLock( 
							lockElement.resource );
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
			if ( ufe != null )
			{
				throw new UndoFailedException( "Undo commands failed, " +
					"undo fail count was " + undoFailedCount, ufe );
			}
		}
		// else a transaction with no commands... ok
	}
	
	public synchronized void dumpStack()
	{
		System.out.print( "Commands in stack: " );
		java.util.Iterator itr = commandStack.keySet().iterator();
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
				commandStack.get( thread ) );
		}
		System.out.println( "TransactionCache size: " + 
			TransactionCache.getCache().size() );
	}
}
		