package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

/**
 * Implementation of the node store.
 */
public class NodeStore extends AbstractStore implements Store
{
	// node store version, each node store should end with this string
	// (byte encoded)
	private static final String VERSION = "NodeStore v0.9.3";
	 
	// in_use(byte)+next_rel_id(int)+next_prop_id(int)
	private static final int RECORD_SIZE = 9;
	 
	/**
	 * See {@link AbstractStore#AbstractStore(String, Map)}
	 */
	public NodeStore( String fileName, Map<?,?> config ) 
		throws IOException
	{
		super( fileName, config );
	}

	/**
	 * See {@link AbstractStore#AbstractStore(String)}
	 */
	public NodeStore( String fileName ) 
		throws IOException
	{
		super( fileName );
	}

	public String getTypeAndVersionDescriptor()
	{
		return VERSION;
	}
	
	public int getRecordSize()
	{
		return RECORD_SIZE;
	}
	
	@Override
	public void close() throws IOException
	{
		super.close();
	}
	
	/**
	 * Creates a new node store contained in <CODE>fileName</CODE> 
	 * If filename is <CODE>null</CODE> or the file already exists an 
	 * <CODE>IOException</CODE> is thrown.
	 *
	 * @param fileName File name of the new node store
	 * @throws IOException If unable to create node store or name null
	 */
	public static void createStore( String fileName ) 
		throws IOException
	{
		createEmptyStore( fileName, VERSION );
		NodeStore store = new NodeStore( fileName );
		NodeRecord nodeRecord = new NodeRecord( store.nextId() );
		nodeRecord.setInUse( true );
		store.updateRecord( nodeRecord );
		store.close();
	}
	
	public NodeRecord getRecord( int id, ReadFromBuffer buffer ) 
		throws IOException
	{
		NodeRecord record;
		if ( buffer != null && !hasWindow( id ) )
		{
			buffer.makeReadyForTransfer();
			getFileChannel().transferTo( ((long) id) * RECORD_SIZE, 
				RECORD_SIZE, buffer.getFileChannel() );
			ByteBuffer buf = buffer.getByteBuffer();
			record = new NodeRecord( id );
			byte inUse = buf.get();
			assert inUse == Record.IN_USE.byteValue();
			record.setInUse( true );
			record.setNextRel( buf.getInt() );
			record.setNextProp( buf.getInt() );
			return record;
		}
		PersistenceWindow window = acquireWindow( id, OperationType.READ );
		try
		{
			record = getRecord( id, window.getBuffer(), false );
			return record;
		}
		finally 
		{
			releaseWindow( window );
		}
	}

	public void updateRecord( NodeRecord record ) throws IOException
	{
		if ( record.isTransferable() && !hasWindow( record.getId() ) )
		{
			if ( transferRecord( record ) )
			{
				if ( !record.inUse()&& !isInRecoveryMode() )
				{
					freeId( record.getId() );
				}
				return;
			}
		}
		PersistenceWindow window = acquireWindow( record.getId(), 
			OperationType.WRITE );
		try
		{
			updateRecord( record, window.getBuffer() );
		}
		finally 
		{
			releaseWindow( window );
		}
	}
	
	public boolean loadLightNode( int id, ReadFromBuffer buffer ) 
		throws IOException 
	{
		NodeRecord record;
		if ( buffer != null && !hasWindow( id ) )
		{
			buffer.makeReadyForTransfer();
			getFileChannel().transferTo( ((long) id) * RECORD_SIZE, 
				RECORD_SIZE, buffer.getFileChannel() );
			ByteBuffer buf = buffer.getByteBuffer();
			record = new NodeRecord( id );
			byte inUse = buf.get();
			if ( inUse != Record.IN_USE.byteValue() )
			{
				return false;
			}
			record.setInUse( true );
			record.setNextRel( buf.getInt() );
			record.setNextProp( buf.getInt() );
			// cache.add( id, record );
			return true;
		}
		PersistenceWindow window = acquireWindow( id, OperationType.READ );
		try
		{
			record = getRecord( id, window.getBuffer(), true );
			if ( !record.inUse() )
			{
				return false;
			}
			return true;
		}
		finally 
		{
			releaseWindow( window );
		}
	}
	
	private NodeRecord getRecord( int id, Buffer buffer, boolean check ) 
		throws IOException
	{
		int offset = (int) ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		boolean inUse = ( buffer.get() == Record.IN_USE.byteValue() );
		if ( !inUse && !check )
		{
			throw new IOException( "Record[" + id + "] not in use" );
		}
		NodeRecord nodeRecord = new NodeRecord( id );
		nodeRecord.setInUse( inUse );
		nodeRecord.setNextRel( buffer.getInt() );
		nodeRecord.setNextProp( buffer.getInt() );
		return nodeRecord;
	}
	
	private boolean transferRecord( NodeRecord record ) throws IOException
	{
		long id = record.getId();
		long count = record.getTransferCount();
		FileChannel fileChannel = getFileChannel();
		fileChannel.position( id * getRecordSize() );
		if ( count != record.getFromChannel().transferTo( 
			record.getTransferStartPosition(), count, fileChannel ) )
		{
			return false;
		}
		return true;
	}
	
	private void updateRecord( NodeRecord record, Buffer buffer )
		throws IOException
	{
		int id = record.getId();
		int offset = (int) ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		if ( record.inUse() )
		{
			buffer.put( Record.IN_USE.byteValue() ).putInt( 
				record.getNextRel() ).putInt( record.getNextProp() );
		}
		else
		{
			buffer.put( Record.NOT_IN_USE.byteValue() );
			if ( !isInRecoveryMode() )
			{
				freeId( id );
			}
		}
	}
	
	public String toString()
	{
		return "NodeStore";
	}
}
