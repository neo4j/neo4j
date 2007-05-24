package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.LinkedList;

/**
 * Makes a {@link PersistenceWindow} "lockable" meaning it can be locked
 * by a thread during a operation making sure no other thread use the same
 * window concurrently. 
 */
abstract class LockableWindow implements PersistenceWindow
{
	public abstract Buffer getBuffer();

	public abstract int position();
	
	public abstract int size();
	
	private OperationType type = null;
	private FileChannel fileChannel = null;

	private Thread lockingThread = null;
	private LinkedList<Thread> waitingThreadList = new LinkedList<Thread>();
	private int lockCount = 0;
	private int marked = 0;
	
	LockableWindow( FileChannel fileChannel )
	{
		this.fileChannel = fileChannel;
	}

	boolean encapsulates( int position )
	{
		return position() <= position && position < position() + size();
	}

	FileChannel getFileChannel() throws IOException
	{
		return fileChannel;
	}
	
	OperationType getOperationType()
	{
		return type;
	}
	
	void setOperationType( OperationType type )
	{
		this.type = type;
	}
	
	synchronized void mark()
	{
		this.marked++;
	}
	
	synchronized boolean isMarked()
	{
		return marked > 0;
	}
	
	synchronized void lock()
	{
		Thread currentThread = Thread.currentThread();
		while ( lockCount > 0 && lockingThread != currentThread )
		{
			waitingThreadList.addFirst( currentThread ); 
			try
			{
				wait();
			}
			catch ( InterruptedException e )
			{
			}
		}
		lockCount++;
		lockingThread = currentThread;
		marked--;
	}
	
	synchronized void unLock()
	{
		Thread currentThread = Thread.currentThread();
		if ( lockCount == 0 )
		{
			throw new RuntimeException( "" + currentThread +
				" don't have window lock on " + this );
		}
		lockCount--;
		if ( lockCount == 0 )
		{
			lockingThread = null;
			if ( waitingThreadList.size() > 0 )
			{
				waitingThreadList.removeLast().interrupt();
			}
		}
	}
	
	synchronized int getWaitingThreadsCount()
	{
		return waitingThreadList.size();
	}
}
