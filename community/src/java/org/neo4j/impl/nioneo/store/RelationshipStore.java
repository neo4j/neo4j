package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

/**
 * Implementation of the relationship store.  
 */
public class RelationshipStore extends AbstractStore implements Store
{
	// relationship store version, each rel store ends with this 
	// string (byte encoded)
	private static final String VERSION = "RelationshipStore v0.9.1";
	 
	// record header size
	// directed|in_use(byte)+first_node(int)+second_node(int)+rel_type(int)+
	// first_prev_rel_id(int)+first_next_rel_id+second_prev_rel_id(int)+
	// second_next_rel_id+next_prop_id(int)
	private static final int RECORD_SIZE = 33;
	
	/**
	 * See {@link AbstractStore#AbstractStore(String, Map)}
	 */
	public RelationshipStore( String fileName, Map config ) 
		throws IOException
	{
		super( fileName, config );
	}

	/**
	 * See {@link AbstractStore#AbstractStore(String)}
	 */
	public RelationshipStore( String fileName ) throws IOException
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
	 * Creates a new relationship store contained in <CODE>fileName</CODE> 
	 * If filename is <CODE>null</CODE> or the file already exists an 
	 * <CODE>IOException</CODE> is thrown.
	 *
	 * @param fileName File name of the new relationship store
	 * @throws IOException If unable to create relationship store or name null
	 */
	public static void createStore( String fileName ) 
		throws IOException
	{
		createEmptyStore( fileName, VERSION );
	}
	
	public RelationshipRecord getRecord( int id, ReadFromBuffer buffer ) 
		throws IOException
	{
		RelationshipRecord record;
		if ( buffer != null && !hasWindow( id ) )
		{
			buffer.makeReadyForTransfer();
			getFileChannel().transferTo( ((long) id) * RECORD_SIZE, 
				RECORD_SIZE, buffer.getFileChannel() );
			ByteBuffer buf = buffer.getByteBuffer();
			byte inUse = buf.get();
			assert inUse == Record.IN_USE.byteValue();
			record = new RelationshipRecord( id, 
				buf.getInt(), buf.getInt(), buf.getInt() );
			record.setInUse( true );
			record.setFirstPrevRel( buf.getInt() );
			record.setFirstNextRel( buf.getInt() );
			record.setSecondPrevRel( buf.getInt() );
			record.setSecondNextRel( buf.getInt() );
			record.setNextProp( buf.getInt() );
			return record;
		}
		PersistenceWindow window = acquireWindow( id, OperationType.READ );
		try
		{
			record = getRecord( id, window.getBuffer() );
			return record;
		}
		finally 
		{
			releaseWindow( window );
		}
	}
	
	public void updateRecord( RelationshipRecord record ) throws IOException
	{
		if ( record.isTransferable() && !hasWindow( record.getId() ) )
		{
			if ( transferRecord( record ) )
			{
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
	
	private boolean transferRecord( RelationshipRecord record ) 
		throws IOException
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
	
	private void updateRecord( RelationshipRecord record, Buffer buffer )
		throws IOException
	{
		int id = record.getId();
		int offset = (int) ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		if ( record.inUse() )
		{
			byte inUse = Record.IN_USE.byteValue();
			buffer.put( inUse ).putInt( record.getFirstNode() ).putInt(  
				record.getSecondNode() ).putInt( record.getType() ).putInt( 
				record.getFirstPrevRel() ).putInt( 
				record.getFirstNextRel() ).putInt( 
				record.getSecondPrevRel() ).putInt( 
				record.getSecondNextRel() ).putInt( record.getNextProp() );
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
	
	private RelationshipRecord getRecord( int id, Buffer buffer ) 
		throws IOException
	{
		int offset = (int) ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		byte inUse = buffer.get();
		boolean inUseFlag = ( ( inUse & Record.IN_USE.byteValue() ) == 
			Record.IN_USE.byteValue() );
		if ( !inUseFlag )
		{
			throw new IOException( "Record[" + id + "] not in use" );
		}
		RelationshipRecord record = new RelationshipRecord( id, 
			buffer.getInt(), buffer.getInt(), buffer.getInt() );
		record.setInUse( inUseFlag );
		record.setFirstPrevRel( buffer.getInt() );
		record.setFirstNextRel( buffer.getInt() );
		record.setSecondPrevRel( buffer.getInt() );
		record.setSecondNextRel( buffer.getInt() );
		record.setNextProp( buffer.getInt() );
		return record;
	}
	
	public String toString()
	{
		return "RelStore";
	}
}
