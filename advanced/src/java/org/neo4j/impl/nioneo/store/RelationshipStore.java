package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
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
	
	private Map<Integer,RelationshipRecord> cache = 
		Collections.synchronizedMap( 
			new HashMap<Integer,RelationshipRecord>() );
	
	private PropertyStore propStore = null;
	
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
	
	void setPropertyStore( PropertyStore propStore )
	{
		this.propStore = propStore;
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
	
	public RelationshipRecord getRecord( int id ) throws IOException
	{
		RelationshipRecord record = cache.get( id );
		if ( record != null )
		{
			if ( !record.inUse() )
			{
				throw new IOException( "Record[" + id + "] not in use" );
			}
			return record;
		}
		PersistenceWindow window = acquireWindow( id, OperationType.READ );
		try
		{
			record = getRecord( id, window.getBuffer(), false );
			cache.put( id, record );
			return record;
		}
		finally 
		{
			releaseWindow( window );
		}
	}
	
	public void updateRecord( RelationshipRecord record ) throws IOException
	{
		PersistenceWindow window = acquireWindow( record.getId(), 
			OperationType.WRITE );
		try
		{
			updateRecord( record, window.getBuffer() );
			cache.remove( record.getId() );
		}
		finally 
		{
			releaseWindow( window );
		}
	}
	
	public PropertyData[] getProperties( int relId )
		throws IOException
	{
		PersistenceWindow window = acquireWindow( relId, OperationType.READ );
		try
		{
			int nextPropertyId = 
				getNextPropertyId( relId, window.getBuffer() );
			if ( nextPropertyId != Record.NO_NEXT_PROPERTY.intValue() )
			{
				return propStore.getProperties( nextPropertyId );
			}
			return new PropertyData[0];
		}
		finally
		{
			releaseWindow( window );
		}
	}

	public RelationshipData getRelationship( int id ) throws IOException
	{
		PersistenceWindow window = acquireWindow( id, OperationType.READ );
		try
		{
			return getRelationship( id, window.getBuffer() );
		}
		finally
		{
			releaseWindow( window );
		}
	}
	
	private void updateRecord( RelationshipRecord record, Buffer buffer )
		throws IOException
	{
		int id = record.getId();
		int offset = ( id - buffer.position() ) * getRecordSize();
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
			buffer.put( Record.NOT_IN_USE.byteValue() ).putInt( 0 ).putInt( 
				0 ).putInt( 0 ).putInt( 
					Record.NO_PREV_RELATIONSHIP.intValue() ).putInt( 
					Record.NO_NEXT_RELATIONSHIP.intValue() ).putInt( 
					Record.NO_PREV_RELATIONSHIP.intValue() ).putInt( 
					Record.NO_NEXT_RELATIONSHIP.intValue() ).putInt( 
					Record.NO_NEXT_PROPERTY.intValue() );
			if ( !isInRecoveryMode() )
			{
				freeId( id );
			}
		}
	}
	
	private RelationshipRecord getRecord( int id, Buffer buffer, 
		boolean checkOnly ) throws IOException
	{
		int offset = ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		byte inUse = buffer.get();
		boolean inUseFlag = ( ( inUse & Record.IN_USE.byteValue() ) == 
			Record.IN_USE.byteValue() );
		if ( !inUseFlag && !checkOnly )
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
	
	private RelationshipData getRelationship( int id, Buffer buffer ) 
		throws IOException
	{
		int offset = ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		// byte inUseAndDirected = buffer.get();
		byte inUse = buffer.get();
		boolean inUseFlag = ( ( inUse & Record.IN_USE.byteValue() ) == 
			Record.IN_USE.byteValue() );
		if ( !inUseFlag )
		{
			throw new IOException( "Record[" + id + "] not in use[" +
				inUse + "]" );
		}
		return new RelationshipData( id, buffer.getInt(), 
			buffer.getInt(), buffer.getInt(), buffer.getInt(), 
			buffer.getInt(), buffer.getInt(), buffer.getInt(), 
			buffer.getInt() );
	}
	
	private int getNextPropertyId( int relId, Buffer buffer ) 
		throws IOException
	{
		int offset = ( relId - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset + 29 );
		return buffer.getInt();
	}
	
	public String toString()
	{
		return "RelStore";
	}
}
