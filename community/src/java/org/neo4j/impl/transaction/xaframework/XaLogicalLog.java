package org.neo4j.impl.transaction.xaframework;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.neo4j.impl.util.ArrayMap;

/**
 * <CODE>XaLogicalLog</CODE> is a transaction and logical log combined. In 
 * this log information about the transaction (such as started, prepared and 
 * committed) will be written. All commands participating in the transaction 
 * will also be written to the log.
 * <p>
 * Normally you don't have to do anything with this log except open it after 
 * it has been instanciated (see {@link XaContainer}). The only method that may 
 * be of use when implementing a XA compatible resource is the 
 * {@link #getCurrentTxIdentifier}. Leave everything else be unless you know 
 * what you're doing.
 * <p>
 * When the log is opened it will be scaned for uncompleted transactions and 
 * those transactions will be re-created. When scan of log is complete all 
 * transactions that hasn't entered prepared state will be marked as done 
 * (implies rolledback) and dropped. All transactions that have been prepared
 * will be held in memory until the transaction manager tells them to commit.
 * Transaction that already started commit but didn't get flagged as done will
 * be re-committed. 
 */
public class XaLogicalLog
{
	private Logger log;
	// empty record due to memory mapped file
	private static final byte EMPTY = (byte) 0;
	// tx has started
	private static final byte TX_START = (byte) 1;
	// tx has been prepared
	private static final byte TX_PREPARE = (byte) 2;
	// a XaCommand in a transaction
	private static final byte COMMAND = (byte) 3;
	// done, either committed and flushed, a read only tx or rolledback
	private static final byte DONE = (byte) 4;
	// tx one-phase commit
	private static final byte TX_1P_COMMIT = (byte) 5;
	
	private FileChannel fileChannel = null;
	private ByteBuffer buffer = null;
	private LogBuffer writeBuffer = null;
	private long logCreated = 0;
	private ArrayMap<Integer,Xid> xidIdentMap = new ArrayMap<Integer,Xid>( 4, 
		false, true );
	private HashMap<Integer,XaTransaction> recoveredTxMap = 
		new HashMap<Integer,XaTransaction>();
	private int nextIdentifier = 1;
	private boolean scanIsComplete = false;

	private	String fileName = null;
	private final XaResourceManager xaRm;
	private final XaCommandFactory cf;
	private final XaTransactionFactory xaTf;
	
	XaLogicalLog( String fileName, XaResourceManager xaRm, XaCommandFactory cf,   
		XaTransactionFactory xaTf ) // throws IOException
	{
		this.fileName = fileName;
		this.xaRm = xaRm;
		this.cf = cf;
		this.xaTf = xaTf;
		log = Logger.getLogger( this.getClass().getName() + "/" + 
			fileName );
	}
	
	void open() throws IOException
	{
		fileChannel = new RandomAccessFile( fileName, "rw" ).getChannel();
 		buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE + 
			Xid.MAXBQUALSIZE * 10 );
		if ( fileChannel.size() != 0 )
		{
			doInternalRecovery();
		}
		else
		{
			scanIsComplete = true;
			logCreated = System.currentTimeMillis();
			buffer.clear();
			buffer.putLong( logCreated );
			buffer.flip();
			fileChannel.write( buffer ); 
		}
		buffer = null;
		writeBuffer = new LogBuffer( fileChannel );
	}
	
	boolean scanIsComplete()
	{
		return scanIsComplete;
	}
	
	private int getNextIdentifier()
	{
		nextIdentifier++;
		if ( nextIdentifier < 0 )
		{
			nextIdentifier = 1;
		}
		return nextIdentifier;
	}

	// returns identifier for transaction
	// [TX_START][xid[gid.length,bid.lengh,gid,bid]][identifier][format id]
	public synchronized int start( Xid xid ) throws XAException
	{
		int xidIdent = getNextIdentifier();
		try
		{
			byte globalId[] = xid.getGlobalTransactionId();
			byte branchId[] = xid.getBranchQualifier();
			int formatId = xid.getFormatId();
			writeBuffer.put( TX_START ).put( ( byte ) globalId.length ).put( 
			( byte ) branchId.length ).put( globalId ).put( branchId ).putInt( 
				xidIdent ).putInt( formatId );
			xidIdentMap.put( xidIdent, xid );
		}
		catch ( IOException e )
		{
			throw new XAException( 
				"Logical log couldn't start transaction: " + e );
		}
		return xidIdent;
	}

	private boolean readTxStartEntry() throws IOException
	{
		// get the global id
		buffer.clear(); buffer.limit( 1 );
		if ( fileChannel.read( buffer ) != buffer.limit() )
		{
			return false;
		}
		buffer.flip();
		byte globalIdLength = buffer.get();
		// get the branchId id
		buffer.clear(); buffer.limit( 1 );
		if ( fileChannel.read( buffer ) != buffer.limit() )
		{
			return false;
		}
		buffer.flip();
		byte branchIdLength = buffer.get();
		byte globalId[] = new byte[ globalIdLength ];
		ByteBuffer tmpBuffer = ByteBuffer.wrap( globalId );
		if ( fileChannel.read( tmpBuffer ) != globalId.length )
		{
			return false;
		}
		byte branchId[] = new byte[ branchIdLength ];
		tmpBuffer = ByteBuffer.wrap( branchId );
		if ( fileChannel.read( tmpBuffer ) != branchId.length )
		{
			return false;
		}
		// get the neo tx identifier
		buffer.clear(); buffer.limit( 4 );
		if ( fileChannel.read( buffer ) != buffer.limit() )
		{
			return false;
		}
		buffer.flip();
		int identifier = buffer.getInt();
		// get the format id
		buffer.clear(); buffer.limit( 4 );
		if ( fileChannel.read( buffer ) != buffer.limit() )
		{
			return false;
		}
		buffer.flip();
		int formatId = buffer.getInt();
		// re-create the transaction
		Xid xid = new XidImpl( globalId, branchId, formatId ); 
		xidIdentMap.put( identifier, xid );
		XaTransaction xaTx = xaTf.create( identifier );
		xaTx.setRecovered();
		recoveredTxMap.put( identifier, xaTx );
		xaRm.injectStart( xid, xaTx );
		return true;
	}
	
	//[TX_PREPARE][identifier]
	public synchronized void prepare( int identifier ) throws XAException
	{
		assert xidIdentMap.get( identifier ) != null;
		try
		{
			writeBuffer.put( TX_PREPARE ).putInt( identifier );
			writeBuffer.force();
		}
		catch ( IOException e )
		{
			throw new XAException( "Logical log unable to mark prepare [" + 
				identifier + "] " + e );
		}
	}
	
	private boolean readTxPrepareEntry() throws IOException
	{
		// get the neo tx identifier
		buffer.clear(); buffer.limit( 4 );
		if ( fileChannel.read( buffer ) != buffer.limit() )
		{
			return false;
		}
		buffer.flip();
		int identifier = buffer.getInt();
		Xid xid = xidIdentMap.get( identifier );
		if ( xid == null )
		{
			return false;
		}
		if ( xaRm.injectPrepare( xid ) )
		{
			// read only we can remove
			xidIdentMap.remove( identifier );
			recoveredTxMap.remove( identifier );
		}
		return true;
	}

	//[TX_1P_COMMIT][identifier]
	public synchronized void commitOnePhase( int identifier ) throws XAException
	{
		assert xidIdentMap.get( identifier ) != null;
		try
		{
			writeBuffer.put( TX_1P_COMMIT ).putInt( identifier );
			writeBuffer.force();
		}
		catch ( IOException e )
		{
			throw new XAException( "Logical log unable to mark 1P-commit [" + 
				identifier + "] " + e );
		}
	}
	
	private boolean readTxOnePhaseCommit() throws IOException
	{
		// get the neo tx identifier
		buffer.clear(); buffer.limit( 4 );
		if ( fileChannel.read( buffer ) != buffer.limit() )
		{
			return false;
		}
		buffer.flip();
		int identifier = buffer.getInt();
		Xid xid = xidIdentMap.get( identifier );
		if ( xid == null )
		{
			return false;
		}
		xaRm.injectOnePhaseCommit( xid );
		return true;
	}
	
	//[DONE][identifier]
	public synchronized void done( int identifier ) throws XAException
	{
		assert xidIdentMap.get( identifier ) != null;
		try
		{
			writeBuffer.put( DONE ).putInt( identifier );
			xidIdentMap.remove( identifier );
		}
		catch ( IOException e )
		{
			throw new XAException( "Logical log unable to mark as done [" + 
				identifier + "] " + e );
		}
	}
	
	//[DONE][identifier] called from XaResourceManager during internal recovery
	synchronized void doneInternal( int identifier ) throws IOException
	{
		buffer.clear();
		buffer.put( DONE ).putInt( identifier );
		buffer.flip();
		fileChannel.write( buffer );
		xidIdentMap.remove( identifier );
	}

	private boolean readDoneEntry() throws IOException
	{
		// get the neo tx identifier
		buffer.clear(); buffer.limit( 4 );
		if ( fileChannel.read( buffer ) != buffer.limit() )
		{
			return false;
		}
		buffer.flip();
		int identifier = buffer.getInt();
		Xid xid = xidIdentMap.get( identifier );
		if ( xid == null )
		{
			return false;
		}
		xaRm.pruneXid( xid );
		xidIdentMap.remove( identifier );
		recoveredTxMap.remove( identifier );
		return true;
	}
	
	//[COMMAND][identifier][COMMAND_DATA]
	public synchronized void writeCommand( XaCommand command, int identifier )
		throws IOException
	{
		assert xidIdentMap.get( identifier ) != null;
		writeBuffer.put( COMMAND ).putInt( identifier );
		command.writeToFile( writeBuffer ); // fileChannel, buffer );
	}
	
	private boolean readCommandEntry() throws IOException
	{
		buffer.clear(); buffer.limit( 4 );
		if ( fileChannel.read( buffer ) != buffer.limit() )
		{
			return false;
		}
		buffer.flip();
		int identifier = buffer.getInt();
		XaCommand command = cf.readCommand( fileChannel, buffer );
		if ( command == null ) 
		{
			// readCommand returns null if full command couldn't be loaded
			return false;
		}
		command.setRecovered();
		XaTransaction xaTx = recoveredTxMap.get( identifier );
		xaTx.injectCommand( command );
		return true;
	}

	// used in testing, truncate log to real size removing any non used memory
	// mapped space
	public synchronized void truncate() throws IOException
	{
		long truncateAt = writeBuffer.getFileChannelPosition();
		writeBuffer.releaseMemoryMapped();
		fileChannel.truncate( truncateAt );
	}
	
	synchronized void makeNewLog()
	{
		// save recovered log
		if ( xidIdentMap.size() > 0 )
		{
			throw new RuntimeException( "Active transactions found: " 
				+ xidIdentMap.size() + ", can't make new log file" );
		}
		WeakReference<MappedByteBuffer> bufferWeakRef = null;
		if ( writeBuffer != null )
		{
			MappedByteBuffer mappedBuffer = writeBuffer.getMappedBuffer();
			if ( mappedBuffer != null )
			{
				bufferWeakRef = new WeakReference<MappedByteBuffer>( 
					mappedBuffer );
				mappedBuffer = null;
			}
			writeBuffer = null;
		}
		try
		{
			fileChannel.close();
		}
		catch ( IOException e )
		{
			throw new RuntimeException( "Unable to close log[" + fileName + 
				"]", e );
		}
		File file = new File( fileName );
		if ( !file.exists() )
		{
			throw new RuntimeException( "Logical log[" + fileName + 
				"] not found" );
		}
		// TODO: if store old logs save them here
		try
		{
			String saveName = fileName + ".recovered-" + 
				System.currentTimeMillis();
			file.renameTo( new File( saveName ) );
		}
		catch ( Exception e )
		{
			boolean renamed = false;
			String saveName = fileName + ".recovered-" + 
				System.currentTimeMillis();
			try
			{
				renamed = file.renameTo( new File( saveName ) );
			} catch ( Exception ee ) {}
			for ( int i = 0; i < 3 && !renamed; i++ )
			{
				// hack for WINBLOWS
				try
				{
					Thread.sleep( 500 );
				} catch ( InterruptedException ee ) 
				{ } // ok
				try
				{
					System.gc();
					renamed = file.delete();
				} 
				catch ( Exception ee ) {} // ok...
			}
			if ( !renamed && bufferWeakRef != null )
			{
				try
				{
					clean( bufferWeakRef.get() );
					renamed = file.delete();
				}
				catch ( Exception ee ) {} // at least we tried...
			}
			
			if ( !renamed )
			{
				throw new RuntimeException( "Unable to rename recovered " + 
					"log file[" + fileName + "]" );
			}
		}
		// create a new one
		try
		{
			this.open();
		}
		catch ( IOException e )
		{
			throw new RuntimeException( "Unable to open new log[" + 
				fileName + "]", e );
		}
	}
	
	public synchronized void close() throws IOException
	{
		if ( xidIdentMap.size() > 0 )
		{
			log.info( "Active transactions: " + xidIdentMap.size() );
			log.info( "Closing dirty log: " + fileName );
			if ( writeBuffer != null )
			{
				writeBuffer.force();
			}
			writeBuffer = null;
			fileChannel.close();
			return;
		}
		WeakReference<MappedByteBuffer> bufferWeakRef = null;
		if ( writeBuffer != null )
		{
			writeBuffer.force();
			MappedByteBuffer mappedBuffer = writeBuffer.getMappedBuffer();
			if ( mappedBuffer != null )
			{
				bufferWeakRef = new WeakReference<MappedByteBuffer>( 
					mappedBuffer );
				mappedBuffer = null;
			}
			writeBuffer = null;
		}
		fileChannel.close();
		File file = new File( fileName );
		if ( !file.exists() )
		{
			throw new IOException( "Logical log[" + fileName + "] not found" );
		}
		// TODO: if store old logs save them here
		boolean deleted = false;
		try
		{
			deleted = file.delete();
		} catch ( Exception e ) {}
		
		// hack for WINBLOWS
		for ( int i = 0; i < 3 && !deleted; i++ )
		{
			try
			{
				Thread.sleep( 500 );
			} catch ( InterruptedException e ) 
			{ } // ok
			try
			{
				System.gc();
				deleted = file.delete();
			} 
			catch ( Exception e ) {} // ok...
		}
		if ( !deleted && bufferWeakRef != null )
		{
			try
			{
				clean( bufferWeakRef.get() );
				deleted = file.delete();
			}
			catch ( Exception e ) {} // at least we tried...
		}
		
		if ( !deleted )
		{
			log.warning( "Unable to delete clean logical log[" +  
					fileName + "]" );
		}
	}
	
	private void doInternalRecovery() throws IOException
	{
		log.info( "Logical log is dirty, this means Neo hasn't been " + 
			"shutdown properly. Recovering..." ); 
		// get log creation time
		buffer.clear(); buffer.limit( 8 ); 
		if ( fileChannel.read( buffer ) != 8 )
		{
			log.info( "Unable to read timestamp information, " +
				"asuming no records in logical log." );
			fileChannel.close();
			new File( fileName ).renameTo( new File( fileName + 
				"_unkown_timestamp_" + System.currentTimeMillis() + ".log" ) );
			fileChannel = new RandomAccessFile( fileName, "rw" ).getChannel();
			return;
		}
		buffer.flip();
		logCreated = buffer.getLong();
		log.fine( "Logical log created " + new java.util.Date( logCreated ) );
		long logEntriesFound = 0;
		while ( readEntry() )
		{ 
			logEntriesFound++;
		}
		scanIsComplete = true;
		log.fine( "Internal recovery completed, scanned " + 
			logEntriesFound + " log entries." );
		log.fine( xidIdentMap.size() + " uncompleted transactions found " );
		xaRm.checkXids();
		if ( xidIdentMap.size() > 0 )
		{
			log.info( "Found " + xidIdentMap.size() + 
				" prepared 2PC transactions." );
		}
		recoveredTxMap.clear();
	}
	
	void removeNonPreparedTx( int identifier )
	{
		xidIdentMap.remove( identifier );
	}
	
	// for testing, do not use!
	void reset()
	{
		xidIdentMap.clear();
		recoveredTxMap.clear();
	}
	
	private boolean readEntry() throws IOException
	{
		buffer.clear();
		buffer.limit( 1 );
		if ( fileChannel.read( buffer ) != buffer.limit() )
		{
			// ok no more entries we're done
			return false;
		}
		buffer.flip();
		byte entry = buffer.get();
		switch ( entry )
		{
			case TX_START: return readTxStartEntry();
			case TX_PREPARE: return readTxPrepareEntry();
			case TX_1P_COMMIT: return readTxOnePhaseCommit();
			case COMMAND: return readCommandEntry();
			case DONE: return readDoneEntry();
			case EMPTY: return false;
			default: throw new IOException( "Internal recovery failed, " + 
				"unkown log entry[" + entry + "]" );
		}
	}
	
	private ArrayMap<Thread,Integer> txIdentMap = 
		new ArrayMap<Thread,Integer>( 5, true, true );
	
	void registerTxIdentifier( int identifier )
	{
		txIdentMap.put( Thread.currentThread(), identifier );
	}
	
	void unregisterTxIdentifier()
	{
		txIdentMap.remove( Thread.currentThread() );
	}
	
	/**
	 * If the current thread is committing a transaction the 
	 * identifier of that {@link XaTransaction} can be obtained invoking this 
	 * method.
	 *
	 * @return the identifier of the transaction committing or
	 * <CODE>-1</CODE> if current thread isn't committing any transaction
	 */
	public int getCurrentTxIdentifier()
	{
		Integer intValue = txIdentMap.get( Thread.currentThread() );
		if ( intValue != null )
		{
			return intValue;
		}
		return -1;
	}
	
	// workaround suggested at sun bug database
	// see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
	private void clean( final MappedByteBuffer mappedBuffer ) throws Exception 
	{
		AccessController.doPrivileged( new PrivilegedAction<Object>() 
		{
			public Object run() 
			{
				if ( mappedBuffer == null )
				{
					return null;
				}
				try 
				{
					Method getCleanerMethod = mappedBuffer.getClass().getMethod( 
						"cleaner", new Class[0]);
					getCleanerMethod.setAccessible(true);
					sun.misc.Cleaner cleaner = (sun.misc.Cleaner)
						getCleanerMethod.invoke( mappedBuffer, new Object[0] );
					cleaner.clean();
				} 
				catch(Exception e) 
				{
					log.log( Level.INFO, 
						"Unable to invoke cleaner method on " + mappedBuffer, 
						e );
				}
				return null;
			}
		} );
	}	
}