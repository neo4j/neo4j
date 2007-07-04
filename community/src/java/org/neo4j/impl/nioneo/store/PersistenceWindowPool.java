package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.neo4j.impl.cache.LruCache;
import org.neo4j.impl.nioneo.xa.TxInfoManager;


/**
 * Manages {@link PersistenceWindow persistence windows} for a store. Each
 * store can configure how much memory it has for  
 * {@link MappedPersistenceWindow memory mapped windows}. This class tries
 * to make the most efficient use of those windows by allocating them in 
 * such a way that the most frequently used records/blocks (be it for read or
 * write operations) are encapsulated by a memory mapped persistence window.
 */
class PersistenceWindowPool
{
	private static final int MAX_BRICK_COUNT = 10000;
	
	private String storeName;
	private int blockSize;
	private FileChannel fileChannel;
	private LruCache<Integer,PersistenceWindow> rowWindowPool = 
		new LruCache<Integer,PersistenceWindow>( "RowWindows", 100 );
	private Map<Integer,LockableWindow> activeRowWindows = 
		new HashMap<Integer,LockableWindow>();
	private Map<Integer,Set<LockableWindow>> txIdentifiers = 
		new HashMap<Integer,Set<LockableWindow>>();
	private int mappedMem = 0;
	private int memUsed = 0;
	private int brickCount = 0;
	private int brickSize = 0;
	private BrickElement brickArray[] = new BrickElement[0];
	private int brickMiss = 0;
	
	private static Logger log = Logger.getLogger( 
		PersistenceWindowPool.class.getName() );
	private static final int REFRESH_BRICK_COUNT = 500;
	
	private int hit = 0;
	private int miss = 0;
	private int switches = 0;
	private int ooe = 0;
	
	/**
	 * Create new pool for a store.
	 * 
	 * @param storeName Name of store that use this pool
	 * @param blockSize The size of each record/block in the store
	 * @param fileChannel A fileChannel to the store
	 * @param mappedMem Number of bytes dedicated to memory mapped windows
	 * @throws IOException If unable to create pool
	 */
	PersistenceWindowPool( String storeName, int blockSize, 
		FileChannel fileChannel, int mappedMem ) throws IOException
	{
		this.storeName = storeName;
		this.blockSize = blockSize;
		this.fileChannel = fileChannel;
		this.mappedMem = mappedMem;
		setupBricks();
		dumpStatus();
	}
	
	/**
	 * Acquires a windows for <CODE>position</CODE> and 
	 * <CODE>operationType</CODE> locking the window preventing other
	 * threads from using it.
	 * 
	 * @param position The position the needs to be encapsulated by the window
	 * @param operationType The type of operation (READ or WRITE)
	 * @return A locked window encapsulating the position
	 * @throws IOException If unable to acquire the window
	 */
	PersistenceWindow acquire( int position, OperationType operationType ) 
		throws IOException
	{
		LockableWindow window = null;
		synchronized ( activeRowWindows )
		{
			int txIdentifier = 
				TxInfoManager.getManager().getCurrentTxIdentifier();
			Set<LockableWindow> windowSet = null;
			if ( txIdentifier != -1 && operationType == OperationType.WRITE )
			{
				windowSet = txIdentifiers.get( txIdentifier );
				if ( windowSet == null )
				{
					windowSet = new HashSet<LockableWindow>();
					txIdentifiers.put( txIdentifier, windowSet );
				}
			}
			if ( brickMiss >= REFRESH_BRICK_COUNT )
			{
				brickMiss = 0;
				refreshBricks();
			}
			if ( brickSize > 0 )
			{
				int brickIndex = position * blockSize / brickSize;
				if ( brickIndex < brickArray.length )
				{
					window = brickArray[brickIndex].getWindow();
					if ( window != null && !window.encapsulates( position ) )
					{
						log.severe( "NIONEO: FOR pos=" + position + 
							" brickIndex=" + brickIndex + " blockSize=" + 
							blockSize + " brickSize=" + brickSize + 
							" window=" + window );
						throw new RuntimeException( "assssert" );
					}
					brickArray[brickIndex].setHit();
					if ( window != null && windowSet != null )
					{
						windowSet.add( window );
					}
				}
				else
				{
					expandBricks();
				}
			}
			if ( window == null )
			{
				miss++;
				brickMiss++;
				window = activeRowWindows.get( position );
				if ( window == null )
				{
					window = ( LockableWindow ) rowWindowPool.get( position );
					if ( window == null )
					{
						PersistenceRow dpw = new PersistenceRow( 
							blockSize, fileChannel );
						dpw.position( position );
						window = dpw;
					}
					activeRowWindows.put( position, window );
				}
			}
			else
			{
				hit++;
			}
			window.mark();
		}
		window.lock();
		window.setOperationType( operationType );
		return window;
	}
	
	void dumpStatistics()
	{
		log.finest( storeName + " hit=" + hit + " miss=" + miss + 
			" switches=" + switches + " ooe=" + ooe );
	}
	
	/**
	 * Releases a window used for an operation back to the pool and unlocks
	 * it so other threads may use it.
	 * 
	 * @param window The window to be released
	 * @throws IOException If unable to release window
	 */
	void release( PersistenceWindow window ) throws IOException
	{
		synchronized ( activeRowWindows )
		{
			if ( window instanceof PersistenceRow )
			{
				PersistenceRow dpw = ( PersistenceRow ) window;
				dpw.writeOut();
				if ( dpw.getWaitingThreadsCount() == 0 && !dpw.isMarked() )
				{
					int key = dpw.position();
					if ( activeRowWindows.remove( key ) == null )
					{
						assert true;
					}
					rowWindowPool.add( key, window );
				}
			}
			( ( LockableWindow ) window ).unLock();
		}
	}
	
	void close() throws IOException
	{
		flushAll();
		synchronized ( activeRowWindows )
		{
			txIdentifiers = null;
			fileChannel = null;
			activeRowWindows = null;
			// cleanRowWindows();
		}
		dumpStatistics();
	}
	
	private void flushAll() throws IOException
	{
		synchronized ( activeRowWindows )
		{
			for ( Set windowSet : txIdentifiers.values() )
			{
				if ( windowSet != null )
				{
					Iterator itr = windowSet.iterator();
					while ( itr.hasNext() )
					{
						( ( MappedPersistenceWindow ) itr.next() ).force();
					}
				}
			}
		}
		fileChannel.force( false );
	}

	/**
	 * Flushes all windows that has had anything to do with 
	 * <CODE>identifier</CODE>.
	 * 
	 * @param identifier The (transaction) identifier
	 * @throws IOException If unable to flush
	 */
	void flush( int identifier ) throws IOException
	{
		synchronized ( activeRowWindows )
		{
			if ( identifier != -1 )
			{
				Set windowSet = txIdentifiers.remove( identifier );
				if ( windowSet != null )
				{
					Iterator itr = windowSet.iterator();
					while ( itr.hasNext() )
					{
						( ( MappedPersistenceWindow ) itr.next() ).force();
					}
				}
			}
		}
		fileChannel.force( false );
	}

	/**
	 * Removes the mapping between windows and <CODE>identifier</CODE>.
	 * 
	 * @param identifier The (transaction) identifier
	 */
	void forget( int identifier )
	{
		synchronized ( activeRowWindows )
		{
			if ( identifier != -1 )
			{
				txIdentifiers.remove( identifier );
			}
		}
	}

	private static class BrickElement
	{
		private int index;
		private int hitCount;
		private LockableWindow window = null;
		
		BrickElement( int index )
		{
			this.index = index;
		}
		
		void setWindow( LockableWindow window )
		{
			this.window = window;
		}
		
		LockableWindow getWindow()
		{
			return window;
		}
		
		int index()
		{
			return index;
		}
		
		void setHit()
		{
			hitCount += 10;
			if ( hitCount < 0 )
			{
				hitCount -= 10;
			}
		}
		
		int getHit()
		{
			return hitCount;
		}
		
		void refresh()
		{
			if ( window == null )
			{
				hitCount /= 1.25;
			}
			else
			{
				hitCount /= 1.15;
			}
		}
		
		public String toString()
		{
			return "" + hitCount + ( window == null ? "x":"o" );
		}
	}

	private void setupBricks() throws IOException
	{
		long fileSize = fileChannel.size();
		if ( blockSize == 0 )
		{
			return;
		}
		if ( mappedMem > 0 && mappedMem < blockSize * 10 )
		{
			logWarn( "Unable to use " + mappedMem + 
				"b as memory mapped windows, need at least " + 
				blockSize * 10 + "b (block size * 10)" );
			logWarn( "Memory mapped windows have been turned off" );
			mappedMem = 0;
			brickCount = 0;
			brickSize = 0;
			return;
		}
		if ( mappedMem > 0 && fileSize > 0 )
		{
			float ratio = ( mappedMem + 0.0f ) / fileSize;
			if ( ratio >= 1 )
			{
				brickSize = mappedMem / 10;
				brickSize = ( brickSize / blockSize ) * blockSize;
				brickCount = (int) fileSize / brickSize;
			}
			else
			{
				brickCount = (int) ( 100.0 / ratio );
				if ( brickCount > MAX_BRICK_COUNT )
				{
					brickCount = MAX_BRICK_COUNT;
				}
				if ( fileSize / brickCount > mappedMem )
				{
					logWarn( "Unable to use " + ( mappedMem / 1024 ) + 
						"kb as memory mapped windows, need at least " + 
						( fileSize / brickCount / 1024 ) + "kb" );
					logWarn( "Memory mapped windows have been turned off" );
					mappedMem = 0;
					brickCount = 0;
					brickSize = 0;
					return;
				}
				brickSize = (int) fileSize / brickCount;
				brickSize = ( brickSize / blockSize ) * blockSize;
				assert brickSize > blockSize;
			}
		}
		else if ( mappedMem > 0 )
		{
			brickSize = mappedMem / 10;
			brickSize = ( brickSize / blockSize ) * blockSize;
		}
		brickArray = new BrickElement[brickCount];
		for ( int i = 0; i < brickCount; i++ )
		{
			BrickElement element = new BrickElement( i );
			if ( mappedMem >= brickCount * brickSize )
			{
				try
				{
					element.setWindow( new MappedPersistenceWindow( 
						i * brickSize / blockSize, blockSize, brickSize, 
						fileChannel ) );
					memUsed += brickSize;
				}
				catch ( MappedMemException e )
				{
					// ok...
					logWarn( "Unable to memory map" );
				}
			}
			brickArray[i] = element;
		}
	}

	private void refreshBricks() throws IOException
	{
		if ( brickSize <= 0 )
		{
			// memory mapped turned off
			return;
		}
		ArrayList<BrickElement> nonMappedBricks = 
			new ArrayList<BrickElement>();
		ArrayList<BrickElement> mappedBricks = new ArrayList<BrickElement>();
		for ( int i = 0; i < brickCount; i++ )
		{
			BrickElement be = brickArray[i];
			if ( be.getWindow() != null )
			{
				mappedBricks.add( be );
			}
			else
			{
				nonMappedBricks.add( be );
			}
			be.refresh();
		}
		Collections.sort( nonMappedBricks, new BrickSorter() );
		Collections.sort( mappedBricks, new BrickSorter() );
		int mappedIndex = 0;
		int nonMappedIndex = nonMappedBricks.size() - 1;
		// fill up unused memory
		while ( memUsed + brickSize <= mappedMem && nonMappedIndex >= 0 )
		{
			BrickElement nonMappedBrick = nonMappedBricks.get( 
				nonMappedIndex-- );
			try
			{
				nonMappedBrick.setWindow( new MappedPersistenceWindow( 
					nonMappedBrick.index() * brickSize / blockSize, 
					blockSize, brickSize, fileChannel ) );
				memUsed += brickSize;
			}
			catch ( MappedMemException e )
			{
				ooe++;
				logWarn( "Unable to memory map" );
			}
		}
		// switch bad mappings
		while ( nonMappedIndex >= 0 && mappedIndex < mappedBricks.size() )
		{
			BrickElement mappedBrick = mappedBricks.get( mappedIndex++ );
			BrickElement nonMappedBrick = nonMappedBricks.get( 
				nonMappedIndex-- );
			if ( mappedBrick.getHit() >= nonMappedBrick.getHit() )
			{
				break;
			}
			LockableWindow window = mappedBrick.getWindow();
			if ( window.getWaitingThreadsCount() == 0 && !window.isMarked() )
			{
				mappedBrick.setWindow( null );
				memUsed -= brickSize;
				try
				{
					nonMappedBrick.setWindow( new MappedPersistenceWindow( 
						nonMappedBrick.index() * brickSize / blockSize, 
						blockSize, brickSize, fileChannel ) );
					memUsed += brickSize;
					switches++;
				}
				catch ( MappedMemException e )
				{
					ooe++;
					logWarn( "Unable to memory map" );
				}
			}
		}
	}
	
	private void expandBricks() throws IOException
	{
		int diff = (int) fileChannel.size() - ( brickCount * brickSize );
		if ( diff > 0 && diff < brickSize )
		{
			byte zeroBuf[] = new byte[ brickSize - diff ];
			java.util.Arrays.fill( zeroBuf, (byte) 0 );
			fileChannel.write( java.nio.ByteBuffer.wrap( zeroBuf ), 
				fileChannel.size() );
		}
		int newBrickCount = (int) fileChannel.size() / brickSize;
		/*if ( newBrickCount > MAX_BRICK_COUNT )
		{
			logWarn( "Max brick count exceeded, need more memory then " + 
				mappedMem ); 
			logWarn( "Memory mapped windows have been turned off" );
			mappedMem = 0;
			brickCount = 0;
			brickSize = 0;
			memUsed = 0;
			brickArray = new BrickElement[0];
			return;
		}*/
		if ( newBrickCount > brickCount )
		{
			BrickElement tmpArray[] = new BrickElement[ newBrickCount ];
			System.arraycopy( brickArray, 0, tmpArray, 0, brickArray.length );
			for ( int i = brickArray.length; i < tmpArray.length; i++ )
			{
				BrickElement be = new BrickElement( i );
				tmpArray[i] = new BrickElement( i );
				if ( memUsed + brickSize <= mappedMem )
				{
					try
					{
						be.setWindow( new MappedPersistenceWindow( 
							i * brickSize / blockSize, blockSize, brickSize, 
							fileChannel ) );
						memUsed += brickSize;
					}
					catch ( MappedMemException e )
					{
						ooe++;
						logWarn( "Unable to memory map" );
					}
				}
			}
			brickArray = tmpArray;
			brickCount = tmpArray.length;
		}
	}
	
	static class BrickSorter implements Comparator<BrickElement>
	{
		public int compare( BrickElement o1, BrickElement o2 )
		{
			return o1.getHit() - o2.getHit();
		}
		
		public boolean equals( Object o )
		{
			if ( o instanceof BrickSorter )
			{
				return true;
			}
			return false;
		}
	
		public int hashCode()
		{
			return 7371;
		}
	}	

	private void dumpStatus() throws IOException
	{
		log.finest( "[" + storeName + "] brickCount=" + brickCount + 
			" brickSize=" + brickSize + "b mappedMem=" + mappedMem + 
			"b (storeSize=" + fileChannel.size() + "b)" );
	}
	
	private void logWarn( String logMessage )
	{
		log.warning( "[" + storeName + "] " + logMessage );
	}
}
