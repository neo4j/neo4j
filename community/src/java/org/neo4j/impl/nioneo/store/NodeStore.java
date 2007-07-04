package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of the node store.
 */
public class NodeStore extends AbstractStore implements Store
{
	// node store version, each node store should end with this string
	// (byte encoded)
	private static final String VERSION = "NodeStore v0.9.1";
	 
	// in_use(byte)+next_rel_id(int)+next_prop_id(int)
	private static final int RECORD_SIZE = 9;
	 
	private Map<Integer,NodeRecord> cache = 
		Collections.synchronizedMap( new HashMap<Integer,NodeRecord>() );

	private PropertyStore propStore = null;
	private RelationshipStore relStore = null;
	
	/**
	 * See {@link AbstractStore#AbstractStore(String, Map)}
	 */
	public NodeStore( String fileName, Map config ) 
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

	void setRelationshipStore( RelationshipStore relStore )
	{
		this.relStore = relStore;
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
	
	public NodeRecord getRecord( int id ) throws IOException
	{
		NodeRecord record = cache.get( id );
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

	public void updateRecord( NodeRecord record ) throws IOException
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
	
	public boolean loadLightNode( int nodeId ) throws IOException 
	{
		PersistenceWindow window = acquireWindow( nodeId, OperationType.READ );
		try
		{
			return checkNode( nodeId, window.getBuffer() );
		}
		finally
		{
			releaseWindow( window );
		}
	}
	
	public PropertyData[] getProperties( int nodeId )
		throws IOException
	{
		PersistenceWindow window = acquireWindow( nodeId, OperationType.READ );
		try
		{
			int nextPropertyId = getNextPropertyId( nodeId, 
				window.getBuffer() );
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
	
	public RelationshipData[] getRelationships( int nodeId )
		throws IOException
	{
		PersistenceWindow window = acquireWindow( nodeId, OperationType.READ );
		int nextRelId = Record.NO_NEXT_RELATIONSHIP.intValue();
		try
		{
			nextRelId = getNextRelationshipId( nodeId, window.getBuffer() );
		}
		finally
		{
			releaseWindow( window );
		}
		ArrayList<RelationshipData> rels = new ArrayList<RelationshipData>();
		while ( nextRelId != Record.NO_NEXT_RELATIONSHIP.intValue() )
		{
			RelationshipData relData = relStore.getRelationship( nextRelId );  
			rels.add( relData );
			if ( relData.firstNode() == nodeId )
			{
				nextRelId = relData.firstNodeNextRelationshipId();
			}
			else if ( relData.secondNode() == nodeId )
			{
				nextRelId = relData.secondNodeNextRelationshipId();
			}
			else
			{
				throw new RuntimeException( "GAH" );
			}
		}
		return rels.toArray( new RelationshipData[ rels.size() ] );
	}
	
	private NodeRecord getRecord( int id, Buffer buffer, boolean 
		checkOnly ) throws IOException
	{
		int offset = ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		boolean inUse = ( buffer.get() == Record.IN_USE.byteValue() );
		if ( !inUse && !checkOnly )
		{
			throw new IOException( "Record[" + id + "] not in use" );
		}
		NodeRecord nodeRecord = new NodeRecord( id );
		nodeRecord.setInUse( inUse );
		nodeRecord.setNextRel( buffer.getInt() );
		nodeRecord.setNextProp( buffer.getInt() );
		return nodeRecord;
	}
	
	private void updateRecord( NodeRecord record, Buffer buffer )
		throws IOException
	{
		int id = record.getId();
		int offset = ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		if ( record.inUse() )
		{
			buffer.put( Record.IN_USE.byteValue() ).putInt( 
				record.getNextRel() ).putInt( record.getNextProp() );
		}
		else
		{
			buffer.put( Record.NOT_IN_USE.byteValue() ).putInt( 
				Record.NO_NEXT_RELATIONSHIP.intValue() ).putInt( 
					Record.NO_NEXT_PROPERTY.intValue() );
			if ( !isInRecoveryMode() )
			{
				freeId( id );
			}
		}
	}
	
	private boolean checkNode( int nodeId, Buffer buffer )
	{
		int offset = ( nodeId - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		if ( buffer.get() != Record.IN_USE.byteValue() )
		{
			return false;
		}
		return true;
	}
	
	private int getNextRelationshipId( int nodeId, Buffer buffer ) 
		throws IOException
	{
		int offset = ( nodeId - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		byte inUse = buffer.get();
		if ( inUse != Record.IN_USE.byteValue() )
		{
			throw new IOException( "Record[" + nodeId + "] not in use[" +
				inUse + "]" );
		}
		return buffer.getInt();
	}
	
	private int getNextPropertyId( int nodeId, Buffer buffer ) 
		throws IOException
	{
		int offset = ( nodeId - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		byte inUse = buffer.get();
		if ( inUse != Record.IN_USE.byteValue() )
		{
			throw new IOException( "Record[" + nodeId + "] not in use[" +
				inUse + "]" );
		}
		buffer.setOffset( offset + 5 );
		return buffer.getInt();
	}
	
	public String toString()
	{
		return "NodeStore";
	}
}
