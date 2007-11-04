package org.neo4j.impl.nioneo.store;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the relationship type store. Uses a dynamic store to 
 * store relationship type names.
 */
public class RelationshipTypeStore extends AbstractStore implements Store
{
	// store version, each store ends with this string (byte encoded)
	private static final String VERSION = "RelationshipTypeStore v0.9.3";
	 
	// record header size
	// in_use(byte)+type_blockId(int)
	private static final int RECORD_SIZE = 5;
	
	private static final int TYPE_STORE_BLOCK_SIZE = 30;
	 
	private DynamicStringStore typeNameStore;
	
	/**
	 * See {@link AbstractStore#AbstractStore(String, Map)}
	 */
	public RelationshipTypeStore( String fileName, Map<?,?> config ) 
		throws IOException
	{
		super( fileName, config );
	}
	
	@Override
	protected void versionFound( String version )
	{
		System.out.println( "Found version: " + version );
		if ( "RelationshipTypeStore v0.9.1".endsWith( version ) )
		{
			try
			{
				convertRelTypes();
			}
			catch ( Exception e )
			{
				e.printStackTrace();
				throw new RuntimeException( 
					"Unable to convert encoding in relationship type store " );
			}
		}
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
	
	public void flushAll() throws IOException
	{
		typeNameStore.flushAll();
		super.flushAll();
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
	
	public Collection<DynamicRecord> allocateTypeNameRecords( int startBlock, 
		char src[] ) throws IOException
	{
		return typeNameStore.allocateRecords( startBlock, src );
	}
	
	public void updateRecord( RelationshipTypeRecord record ) 
		throws IOException
	{
		if ( record.isTransferable() && !hasWindow( record.getId() ) )
		{
			if ( !transferRecord( record ) )
			{
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
			else
			{
				if ( !record.inUse()&& !isInRecoveryMode() )
				{
					freeId( record.getId() );
				}
			}
		}
		else
		{
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
		for ( DynamicRecord typeRecord : record.getTypeRecords() )
		{
			typeNameStore.updateRecord( typeRecord );
		}
	}
	
	private boolean transferRecord( RelationshipTypeRecord record ) 
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
	
	public RelationshipTypeRecord getRecord( int id, ReadFromBuffer buffer ) 
		throws IOException
	{
		RelationshipTypeRecord record;
		if ( buffer != null && !hasWindow( id ) )
		{
			buffer.makeReadyForTransfer();
			getFileChannel().transferTo( ((long) id) * RECORD_SIZE, 
				RECORD_SIZE, buffer.getFileChannel() );
			ByteBuffer buf = buffer.getByteBuffer();
			byte inUse = buf.get();
			assert inUse == Record.IN_USE.byteValue();
			record = new RelationshipTypeRecord( id );
			record.setInUse( true );
			record.setTypeBlock( buf.getInt() );
		}
		else
		{
			PersistenceWindow window = acquireWindow( id, OperationType.READ );
			try
			{
				record = getRecord( id, window.getBuffer() );
				// cache.add( id, record );
			}
			finally 
			{
				releaseWindow( window );
			}
		}
		Collection<DynamicRecord> nameRecords = 
			typeNameStore.getRecords( record.getTypeBlock(), buffer );
		for ( DynamicRecord nameRecord : nameRecords )
		{
			record.addTypeRecord( nameRecord );
		}
		return record;
	}
	
	public RelationshipTypeData getRelationshipType( int id ) 
		throws IOException
	{
		RelationshipTypeRecord record = getRecord( id, 
			(ReadFromBuffer) null );
		String name = getStringFor( record, null );
		return new RelationshipTypeData( id, name );
	}
	
	public RelationshipTypeData[] getRelationshipTypes()
		throws IOException
	{
		LinkedList<RelationshipTypeData> typeDataList = 
			new LinkedList<RelationshipTypeData>();
		for ( int i = 0; ; i++ )
		{
			// int blockId = -1;
			RelationshipTypeRecord record;
			try
			{
				record = getRecord( i, (ReadFromBuffer) null );
			}
			catch ( IOException e )
			{
				break;
			}
			if ( record.getTypeBlock() != Record.RESERVED.intValue() )
			{
				String name = getStringFor( record, null );
				typeDataList.add( new RelationshipTypeData( i, name ) );						
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
		int offset = (int) ( id - buffer.position() ) * getRecordSize();
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
		int offset = (int) ( id - buffer.position() ) * getRecordSize();
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
		int offset = (int) ( id - buffer.position() ) * getRecordSize();
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

	public String getStringFor( RelationshipTypeRecord relTypeRecord, 
		ReadFromBuffer buffer ) throws IOException
    {
		int recordToFind = relTypeRecord.getTypeBlock();
		Iterator<DynamicRecord> records = 
			relTypeRecord.getTypeRecords().iterator();
		List<char[]> charList = new LinkedList<char[]>();
		int totalSize = 0;
		while ( recordToFind != Record.NO_NEXT_BLOCK.intValue() && 
			records.hasNext() )
		{
			DynamicRecord record = records.next();
			if ( record.inUse() && record.getId() == recordToFind )
			{
				if ( record.isLight() )
				{
					typeNameStore.makeHeavy( record, buffer );
				}
				if ( !record.isCharData() )
				{
					ByteBuffer buf = ByteBuffer.wrap( record.getData() );
					char[] chars = new char[ record.getData().length / 2 ];
					totalSize += chars.length;
					buf.asCharBuffer().get( chars );
					charList.add( chars );
				}
				else
				{
					charList.add( record.getDataAsChar() );
				}
				recordToFind = record.getNextBlock();
				// TODO: optimize here, high chance next is right one
				records = relTypeRecord.getTypeRecords().iterator();
			}
		}
		StringBuffer buf = new StringBuffer();
		for ( char[] str : charList )
		{
			buf.append( str );
		}
		return buf.toString();
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
	
	private void convertRelTypes() throws IOException
	{
		FileChannel fileChannel = getFileChannel();
		DynamicStringStore typeNameStore = new DynamicStringStore( 
			getStorageFileName() + ".names" );
		typeNameStore.makeStoreOk();
		// in_use(byte)+type_blockId(int)
		System.out.println( "Converting encoding on relationship type names" );
		ByteBuffer buffer = ByteBuffer.allocate( 5 );
		fileChannel.position( 0 );
		int i = 0;
		int reservedCount = 1;
		int lastOkPos = 0;
		while ( fileChannel.read( buffer ) == 5 )
		{
			buffer.flip();
			byte inUse = buffer.get();
			int block = buffer.getInt();
			String name = "RESERVED";
			if ( block == -1 )
			{
				name = name + reservedCount++;
			}
			else
			{
				try
				{
					Collection<DynamicRecord> records = 
						typeNameStore.getRecords( block, null );
					name = getOldStringFor( records, block );
					for ( DynamicRecord record : records )
					{
						record.setInUse( false );
						typeNameStore.updateRecord( record );
					}
					int nextId = typeNameStore.nextBlockId();
					char[] chars = new char[ name.length() ];
					name.getChars( 0, name.length(), chars, 0 );
					records = typeNameStore.allocateRecords( nextId, chars );
					for ( DynamicRecord record : records )
					{
						typeNameStore.updateRecord( record );
					}
					buffer.flip();
					buffer.putInt( nextId );
					buffer.flip();
					fileChannel.position( i * 5 + 1 );
					fileChannel.write( buffer );
					lastOkPos = i;
				}
				catch ( IOException e )
				{
					// e.printStackTrace();
					name = null;
				}
			}
			System.out.println( "ID[" + i + "] use[" + inUse + 
				"] blockId[" + block + "] name[" + name + "]" ); 
			i++;
			buffer.clear();
		}
		typeNameStore.close();
		fileChannel.truncate( (lastOkPos + 1 ) * 5 );
	}
	
	private static String getOldStringFor( 
		Collection<DynamicRecord> recordsCol, int startBlock ) 
		throws IOException
    {
		int recordToFind = startBlock;
		Iterator<DynamicRecord> records = recordsCol.iterator();
		List<byte[]> byteList = new LinkedList<byte[]>();
		int totalSize = 0;
		while ( recordToFind != Record.NO_NEXT_BLOCK.intValue() && 
			records.hasNext() )
		{
			DynamicRecord record = records.next();
			if ( record.inUse() && record.getId() == recordToFind )
			{
				if ( !record.isCharData() )
				{
					ByteBuffer buf = ByteBuffer.wrap( record.getData() );
					byte[] bytes = new byte[ record.getData().length ];
					totalSize += bytes.length;
					buf.get( bytes );
					byteList.add( bytes );
				}
				else
				{
					throw new RuntimeException();
					// charList.add( record.getDataAsChar() );
				}
				recordToFind = record.getNextBlock();
				// TODO: make opti here, high chance next is right one
				records = recordsCol.iterator();
			}
		}
		byte[] allBytes = new byte[totalSize];
		int position = 0;
		for ( byte[] bytes : byteList )
		{
			System.arraycopy( bytes, 0, allBytes, position, bytes.length );
			position += bytes.length;
		}
		return new String( allBytes );
    }	
}
