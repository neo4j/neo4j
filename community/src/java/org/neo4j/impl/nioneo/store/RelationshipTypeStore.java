package org.neo4j.impl.nioneo.store;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * Implementation of the relationship type store. Uses a dynamic store to 
 * store relationship type names.
 */
public class RelationshipTypeStore extends AbstractStore implements Store
{
	// store version, each store ends with this string (byte encoded)
	private static final String VERSION = "RelationshipTypeStore v0.9.1";
	 
	// record header size
	// in_use(byte)+type_blockId(int)
	private static final int RECORD_SIZE = 5;
	
	private static final int TYPE_STORE_BLOCK_SIZE = 30;
	 
	private DynamicStringStore typeNameStore;
	
	/**
	 * See {@link AbstractStore#AbstractStore(String, Map)}
	 */
	public RelationshipTypeStore( String fileName, Map config ) 
		throws IOException
	{
		super( fileName, config );
	}

	/**
	 * See {@link AbstractStore#AbstractStore(String)}
	 */
	public RelationshipTypeStore( String fileName ) throws IOException
	{
		super( fileName );
	}
	
	@Override
	protected void initStorage() 
		throws IOException
	{
		typeNameStore = new DynamicStringStore( 
			getStorageFileName() + ".names", getConfig() );
	}
	
	@Override
	protected void closeStorage() throws IOException
	{
		typeNameStore.close();
		typeNameStore = null;
	}

	@Override
	public void flush( int txIdentifier ) throws IOException
	{
		typeNameStore.flush( txIdentifier );
		super.flush( txIdentifier );
	}
	
	@Override
	public void forget( int txIdentifier )
	{
		typeNameStore.forget( txIdentifier );
		super.forget( txIdentifier );
	}

	public String getTypeAndVersionDescriptor()
	{
		return VERSION;
	}
	
	public int getRecordSize()
	{
		return RECORD_SIZE;
	}
	
	/**
	 * Creates a new relationship type store contained in <CODE>fileName</CODE> 
	 * If filename is <CODE>null</CODE> or the file already exists an 
	 * <CODE>IOException</CODE> is thrown.
	 *
	 * @param fileName File name of the new relationship type store
	 * @throws IOException If unable to create store or name null
	 */
	public static void createStore( String fileName ) 
		throws IOException
	{
		createEmptyStore( fileName, VERSION );
		DynamicStringStore.createStore( fileName + ".names", 
			TYPE_STORE_BLOCK_SIZE );
		RelationshipTypeStore store = new RelationshipTypeStore( fileName );
		store.markAsReserved( store.nextId() );
		store.markAsReserved( store.nextId() );
		store.markAsReserved( store.nextId() );
		store.close();
	}
	
	void markAsReserved( int id ) throws IOException
	{
		PersistenceWindow window = acquireWindow( id, OperationType.WRITE );
		try
		{
			markAsReserved( id, window.getBuffer() );
		}
		finally
		{
			releaseWindow( window );
		}
	}
	
	public RelationshipTypeRecord getRecord( int id ) throws IOException
	{
		PersistenceWindow window = acquireWindow( id, OperationType.READ );
		try
		{
			RelationshipTypeRecord record = 
				getRecord( id, window.getBuffer() );
			Collection<DynamicRecord> typeNameRecords = 
				typeNameStore.getRecords( record.getTypeBlock() );
			for ( DynamicRecord typeRecord : typeNameRecords )
			{
				record.addTypeRecord( typeRecord );
			}
			return record;
		}
		finally 
		{
			releaseWindow( window );
		}
	}

	public Collection<DynamicRecord> allocateTypeNameRecords( int startBlock, 
		byte src[] ) throws IOException
	{
		return typeNameStore.allocateRecords( startBlock, src );
	}
	
	public void updateRecord( RelationshipTypeRecord record ) 
		throws IOException
	{
		PersistenceWindow window = acquireWindow( record.getId(), 
			OperationType.WRITE );
		try
		{
			updateRecord( record, window.getBuffer() );
			for ( DynamicRecord typeRecord : record.getTypeRecords() )
			{
				typeNameStore.updateRecord( typeRecord );
			}
		}
		finally 
		{
			releaseWindow( window );
		}
	}
	
	public RelationshipTypeData getRelationshipType( int id ) 
		throws IOException
	{
		PersistenceWindow window = acquireWindow( id, OperationType.READ );
		try
		{
			int typeNameBlockId = getRelationshipTypeBlockId( id, 
				window.getBuffer() ); 
			String name = typeNameStore.getString( typeNameBlockId );  
			return new RelationshipTypeData( id, name );
		}
		finally
		{
			releaseWindow( window );
		}
	}
	
	public RelationshipTypeData[] getRelationshipTypes()
		throws IOException
	{
		LinkedList<RelationshipTypeData> typeDataList = 
			new LinkedList<RelationshipTypeData>();
		for ( int i = 0; ; i++ )
		{
			int blockId = -1;
			try
			{
				PersistenceWindow window = acquireWindow( i, 
					OperationType.READ );
				try
				{
					blockId = getRelationshipTypeBlockId( i, 
						window.getBuffer() );
				}
				finally
				{
					releaseWindow( window );
				}
			}
			catch ( IOException e )
			{
				break;
			}
			if ( blockId != Record.RESERVED.intValue() )
			{
				typeDataList.add( new RelationshipTypeData( 
					i, typeNameStore.getString( blockId ) ) );						
			}
		}
		return typeDataList.toArray( 
			new RelationshipTypeData[ typeDataList.size() ] );
	}
	
	public int nextBlockId() throws IOException
	{
		return typeNameStore.nextBlockId();
	}
	
	public void freeBlockId( int id ) throws IOException
	{
		typeNameStore.freeBlockId( id );
	}
	
	private void markAsReserved( int id, Buffer buffer ) throws IOException
	{
		int offset = ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		if ( buffer.get() != Record.NOT_IN_USE.byteValue() )
		{
			throw new IOException( "Record[" + id + "] already in use" );
		}
		buffer.setOffset( offset );
		buffer.put( Record.IN_USE.byteValue() ).putInt( 
			Record.RESERVED.intValue() );
	}
	
	private RelationshipTypeRecord getRecord( int id, Buffer buffer ) 
		throws IOException
	{
		int offset = ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		if ( buffer.get() != Record.IN_USE.byteValue() )
		{
			throw new IOException( "Record[" + id + "] not in use" );
		}
		RelationshipTypeRecord record = new RelationshipTypeRecord( id );
		record.setInUse( true );
		record.setTypeBlock( buffer.getInt() );
		return record;
	}
	
	private void updateRecord( RelationshipTypeRecord record, Buffer buffer )
	{
		int id = record.getId();
		int offset = ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		if ( record.inUse() )
		{
			buffer.put( Record.IN_USE.byteValue() ).putInt( 
				record.getTypeBlock() );
		}
		else
		{
			buffer.put( Record.NOT_IN_USE.byteValue() ).putInt( 0 );
		}
	}
	
	private int getRelationshipTypeBlockId( int id, Buffer buffer ) 
		throws IOException 
	{
		int offset = ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		if ( buffer.get() != Record.IN_USE.byteValue() )
		{
			throw new IOException( "Record[" + id + "] not in use" );
		}
		return buffer.getInt();
	}
	
	@Override
	protected void rebuildIdGenerator() throws IOException
	{
		logger.info( "Rebuilding id generator for[" + getStorageFileName() + 
		"] ..." );
		closeIdGenerator();
		File file = new File( getStorageFileName() + ".id" );
		if ( file.exists() )
		{
			file.delete();
		}
		IdGenerator.createGenerator( getStorageFileName() + ".id" );
		openIdGenerator();
		FileChannel fileChannel = getFileChannel();
		long fileSize = fileChannel.size();
		int recordSize = getRecordSize();
//		long dot = fileSize / recordSize / 20;
		ByteBuffer byteBuffer = ByteBuffer.wrap( new byte[ recordSize ] );
		int highId = -1;
		for ( int i = 0; i * recordSize < fileSize; i++ )
		{
			fileChannel.read( byteBuffer, i * recordSize );
			byteBuffer.flip();
			byte inUse = byteBuffer.get();
			byteBuffer.flip();
			if ( inUse != Record.IN_USE.byteValue() )
			{
				// hole found, marking as reserved
				byteBuffer.clear();
				byteBuffer.put( Record.IN_USE.byteValue() ).putInt( 
					Record.RESERVED.intValue() );
				byteBuffer.flip();
				fileChannel.write( byteBuffer, i * recordSize );
				byteBuffer.clear();
			}
			else
			{
				highId = i;
			}
			nextId();
//			if ( dot != 0 && i % dot == 0 )
//			{
//				System.out.print( "." );
//			}
		}
		highId++;
		setHighId( highId );
		fileChannel.truncate( highId * recordSize ); 
		logger.info( "[" + getStorageFileName() + "] high id=" + getHighId() );  
		closeIdGenerator();
		openIdGenerator();
	}

	@Override
	public void makeStoreOk() throws IOException
	{
		typeNameStore.makeStoreOk();
		super.makeStoreOk();
	}
	
	@Override
	public void validate()
	{
		typeNameStore.validate();
		super.validate();
	}
}
