package org.neo4j.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * Implementation of the property store. This implementation has two dynamic 
 * stores. One used to store keys and another for string property values.
 */
public class PropertyStore extends AbstractStore implements Store
{
	// store version, each store ends with this string (byte encoded)
	private static final String VERSION = "PropertyStore v0.9.3";
	 
	// record header size
	// in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
	// prev_prop_id(int)+next_prop_id(int)
	private static final int RECORD_SIZE = 25;
	
	private static final int STRING_STORE_BLOCK_SIZE = 30;
	
	private DynamicStringStore stringPropertyStore;
	private PropertyIndexStore propertyIndexStore;
	private DynamicArrayStore arrayPropertyStore;
	
	/**
	 * See {@link AbstractStore#AbstractStore(String, Map)}
	 */
	public PropertyStore( String fileName, Map config ) 
		throws IOException
	{
		super( fileName, config );
	}

	/**
	 * See {@link AbstractStore#AbstractStore(String)}
	 */
	public PropertyStore( String fileName ) throws IOException
	{
		super( fileName );
	}
	
	@Override
	protected void initStorage() throws IOException
	{
		stringPropertyStore = new DynamicStringStore( 
			getStorageFileName() + ".strings", getConfig() );
		File indexStoreFile = new File( getStorageFileName() + ".index" );
		if ( !indexStoreFile.exists() )
		{
			convertKeyToIndexStore();
		}
		propertyIndexStore = new PropertyIndexStore( 
			getStorageFileName() + ".index", getConfig() );
		File arrayStoreFile = new File( getStorageFileName() + ".arrays" );
		// old store, create array store
		if ( !arrayStoreFile.exists() )
		{
			DynamicArrayStore.createStore( getStorageFileName() + ".arrays", 
				STRING_STORE_BLOCK_SIZE );
		}
		arrayPropertyStore = new DynamicArrayStore( 
			getStorageFileName() + ".arrays", getConfig() );
	}
	
	@Override
	protected void closeStorage() throws IOException
	{
		stringPropertyStore.close();
		stringPropertyStore = null;
		propertyIndexStore.close();
		propertyIndexStore = null;
		arrayPropertyStore.close();
		arrayPropertyStore = null;
	}
	
	@Override
	public void flush( int txIdentifier ) throws IOException
	{
		stringPropertyStore.flush( txIdentifier );
		propertyIndexStore.flush( txIdentifier );
		arrayPropertyStore.flush( txIdentifier );
		super.flush( txIdentifier );
	}
	
	@Override
	public void flushAll() throws IOException
	{
		stringPropertyStore.flushAll();
		propertyIndexStore.flushAll();
		arrayPropertyStore.flushAll();
		super.flushAll();
	}
	
	@Override
	public void forget( int txIdentifier )
	{
		stringPropertyStore.forget( txIdentifier );
		propertyIndexStore.forget( txIdentifier );
		arrayPropertyStore.forget( txIdentifier );
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
	 * Creates a new property store contained in <CODE>fileName</CODE> 
	 * If filename is <CODE>null</CODE> or the file already exists an 
	 * <CODE>IOException</CODE> is thrown.
	 *
	 * @param fileName File name of the new property store
	 * @throws IOException If unable to create property store or name null
	 */
	public static void createStore( String fileName ) 
		throws IOException
	{
		createEmptyStore( fileName, VERSION );
		DynamicStringStore.createStore( fileName + ".strings", 
			STRING_STORE_BLOCK_SIZE );
		PropertyIndexStore.createStore( fileName + ".index" ); 
		DynamicArrayStore.createStore( fileName + ".arrays", 
			STRING_STORE_BLOCK_SIZE );
	}
	
	private int nextStringBlockId() throws IOException
	{
		return stringPropertyStore.nextBlockId();
	}
	
	public void freeStringBlockId( int blockId ) throws IOException
	{
		stringPropertyStore.freeBlockId( blockId );
	}
	
	private int nextArrayBlockId() throws IOException
	{
		return arrayPropertyStore.nextBlockId();
	}
	
	public void freeArrayBlockId( int blockId ) throws IOException
	{
		arrayPropertyStore.freeBlockId( blockId );
	}
	
	public PropertyIndexStore getIndexStore()
	{
		return propertyIndexStore;
	}
	
	public void updateRecord( PropertyRecord record ) throws IOException
	{
		if ( record.isTransferable() && !hasWindow( record.getId() ) )
		{
			transferRecord( record );
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
		if ( !record.isLight() )
		{
			for ( DynamicRecord valueRecord : record.getValueRecords() )
			{
				if ( valueRecord.getType() == PropertyType.STRING.intValue() )
				{
					stringPropertyStore.updateRecord( valueRecord );
				}
				else if ( valueRecord.getType() 
					== PropertyType.ARRAY.intValue() )
				{
					arrayPropertyStore.updateRecord( valueRecord );
				}
				else
				{
					throw new RuntimeException( "Unkown dynamic record" );
				}
			}
		}
	}
	
	private void transferRecord( PropertyRecord record ) throws IOException
	{
		int id = record.getId();
		long count = record.getTransferCount();
		FileChannel fileChannel = getFileChannel();
		fileChannel.position( id * getRecordSize() );
		if ( count != record.getFromChannel().transferTo( 
			record.getTransferStartPosition(), count, fileChannel ) )
		{
			throw new RuntimeException( "expected " + count + 
				" bytes transfered" );
		}
	}
	
	// in_use(byte)+type(int)+key_blockId(int)+prop_blockId(long)+
	// prev_prop_id(int)+next_prop_id(int)
	
	private void updateRecord( PropertyRecord record, Buffer buffer )
		throws IOException
	{
		int id = record.getId();
		int offset = ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		if ( record.inUse() )
		{
			buffer.put( Record.IN_USE.byteValue() ).putInt( 
				record.getType().intValue() ).putInt( record.getKeyIndexId() 
				).putLong( record.getPropBlock() ).putInt( 
				record.getPrevProp() ).putInt( record.getNextProp() );
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
	
	public PropertyRecord getLightRecord( int id, ReadFromBuffer buffer ) 
		throws IOException
	{
		PropertyRecord record;
		if ( buffer != null && !hasWindow( id ) )
		{
			buffer.makeReadyForTransfer();
			getFileChannel().transferTo( id * RECORD_SIZE, RECORD_SIZE, 
				buffer.getFileChannel() );
			ByteBuffer buf = buffer.getByteBuffer();
			byte inUse = buf.get();
			assert inUse == Record.IN_USE.byteValue();
			record = new PropertyRecord( id ); 
			record.setType( getEnumType( buf.getInt() ) );
			record.setInUse( true );
			record.setKeyIndexId( buf.getInt() );
			record.setPropBlock( buf.getLong() );
			record.setPrevProp( buf.getInt() );
			record.setNextProp( buf.getInt() );
			return record;
		}
		PersistenceWindow window = acquireWindow( id, OperationType.READ );
		try
		{
			record = getRecord( id, window.getBuffer() );
			record.setIsLight( true );
			return record;
		}
		finally 
		{
			releaseWindow( window );
		}
	}
	
	public void makeHeavy( PropertyRecord record, ReadFromBuffer buffer ) 
		throws IOException
	{
		record.setIsLight( false );
		if ( record.getType() == PropertyType.STRING )
		{
			Collection<DynamicRecord> stringRecords = 
				stringPropertyStore.getLightRecords( 
					( int ) record.getPropBlock(), buffer );
			for ( DynamicRecord stringRecord : stringRecords )
			{
				stringRecord.setType( PropertyType.STRING.intValue() );
				record.addValueRecord( stringRecord );
			}
		}
		else if ( record.getType() == PropertyType.ARRAY )
		{
			Collection<DynamicRecord> arrayRecords = 
				arrayPropertyStore.getLightRecords( 
					( int ) record.getPropBlock(), buffer );
			for ( DynamicRecord arrayRecord : arrayRecords )
			{
				arrayRecord.setType( PropertyType.ARRAY.intValue() );
				record.addValueRecord( arrayRecord );
			}
		}
	}
	
	public PropertyRecord getRecord( int id, ReadFromBuffer buffer ) 
		throws IOException
	{
		PropertyRecord record;
		if ( buffer != null && !hasWindow( id ) )
		{
			buffer.makeReadyForTransfer();
			getFileChannel().transferTo( id * RECORD_SIZE, RECORD_SIZE, 
				buffer.getFileChannel() );
			ByteBuffer buf = buffer.getByteBuffer();
			byte inUse = buf.get();
			assert inUse == Record.IN_USE.byteValue();
			record = new PropertyRecord( id ); 
			record.setType( getEnumType( buf.getInt() ) );
			record.setInUse( true );
			record.setKeyIndexId( buf.getInt() );
			record.setPropBlock( buf.getLong() );
			record.setPrevProp( buf.getInt() );
			record.setNextProp( buf.getInt() );
		}
		else
		{
			PersistenceWindow window = acquireWindow( id, OperationType.READ );
			try
			{
				record = getRecord( id, window.getBuffer() );
			}
			finally 
			{
				releaseWindow( window );
			}
		}
		if ( record.getType() == PropertyType.STRING )
		{
			Collection<DynamicRecord> stringRecords = 
				stringPropertyStore.getLightRecords( 
					(int) record.getPropBlock(), buffer );
			record.setIsLight( false );
			for ( DynamicRecord stringRecord : stringRecords )
			{
				stringRecord.setType( PropertyType.STRING.intValue() );
				record.addValueRecord( stringRecord );
			}
		}
		else if ( record.getType() == PropertyType.ARRAY )
		{
			Collection<DynamicRecord> arrayRecords = 
				arrayPropertyStore.getLightRecords( 
					(int) record.getPropBlock(), buffer );
			record.setIsLight( false );
			for ( DynamicRecord arrayRecord : arrayRecords )
			{
				arrayRecord.setType( PropertyType.ARRAY.intValue() );
				record.addValueRecord( arrayRecord );
			}
		}
		return record;
	}
	
	private PropertyRecord getRecord( int id, Buffer buffer ) 
		throws IOException
	{
		int offset = ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		if ( buffer.get() != Record.IN_USE.byteValue() )
		{
			throw new IOException( "Record[" + id + "] not in use" );
		}
		PropertyRecord record = new PropertyRecord( id ); 
		record.setType( getEnumType( buffer.getInt() ) );
		record.setInUse( true );
		record.setKeyIndexId( buffer.getInt() );
		record.setPropBlock( buffer.getLong() );
		record.setPrevProp( buffer.getInt() );
		record.setNextProp( buffer.getInt() );
		return record;
	}
	
	private PropertyType getEnumType( int type ) throws IOException
	{
		switch ( type )
		{
			case 1: return PropertyType.INT; 
			case 2: return PropertyType.STRING;
			case 3:	return PropertyType.BOOL; 
			case 4: return PropertyType.DOUBLE;
			case 5: return PropertyType.FLOAT; 
			case 6: return PropertyType.LONG;
			case 7: return PropertyType.BYTE;
			case 8: return PropertyType.CHAR;
			case 9: return PropertyType.ARRAY;
			default: throw new IOException( "Unkown enum type:" +
				type );
		}
	}

	@Override
	public void makeStoreOk() throws IOException
	{
		propertyIndexStore.makeStoreOk();
		stringPropertyStore.makeStoreOk();
		arrayPropertyStore.makeStoreOk();
		super.makeStoreOk();
	}
	
	@Override
	public void validate()
	{
		propertyIndexStore.validate();
		stringPropertyStore.validate();
		arrayPropertyStore.validate();
		super.validate();
	}

	private Collection<DynamicRecord> allocateStringRecords( int valueBlockId, 
		char[] chars ) throws IOException
	{
		return stringPropertyStore.allocateRecords( valueBlockId, chars );
	}
	
	private Collection<DynamicRecord> allocateArrayRecords( int valueBlockId, 
		Object array ) throws IOException
	{
		return arrayPropertyStore.allocateRecords( valueBlockId, array );
	}

	public void encodeValue( PropertyRecord record, 
		Object value ) throws IOException
	{
		if ( value instanceof String )
		{
			int stringBlockId = nextStringBlockId();
			record.setPropBlock( stringBlockId );
			String string = (String) value;
			int length = string.length();
			char[] chars = new char[length];
			string.getChars( 0, length, chars, 0 );
			Collection<DynamicRecord> valueRecords = 
				allocateStringRecords( stringBlockId, chars );
			for ( DynamicRecord valueRecord : valueRecords )
			{
				valueRecord.setType( PropertyType.STRING.intValue() );
				record.addValueRecord( valueRecord );
			}
			record.setType( PropertyType.STRING );
		}
		else if ( value instanceof Integer )
		{
			record.setPropBlock( ( ( Integer ) value ).intValue() );
			record.setType( PropertyType.INT );
		}
		else if ( value instanceof Boolean )
		{
			record.setPropBlock(  
				( ( ( Boolean ) value ).booleanValue() ? 1 : 0 ) );
			record.setType( PropertyType.BOOL );
		}
		else if ( value instanceof Float )
		{
			record.setPropBlock(  Float.floatToRawIntBits( 
				( ( Float ) value ).floatValue() ) );
			record.setType( PropertyType.FLOAT );
		}
		else if ( value instanceof Long )
		{
			record.setPropBlock( ( ( Long ) value ).longValue() );
			record.setType( PropertyType.LONG );
		}
		else if ( value instanceof Double )
		{
			record.setPropBlock( Double.doubleToRawLongBits( 
				( ( Double ) value ).doubleValue() ) );
			record.setType( PropertyType.DOUBLE );
		}
		else if ( value instanceof Byte )
		{
			record.setPropBlock( (( Byte) value ).byteValue() );
			record.setType( PropertyType.BYTE );
		}
		else if ( value instanceof Character )
		{
			record.setPropBlock( (( Character) value ).charValue() );
			record.setType( PropertyType.CHAR );
		}
		else if ( value.getClass().isArray() )
		{
			int arrayBlockId = nextArrayBlockId();
			record.setPropBlock( arrayBlockId );
			Collection<DynamicRecord> arrayRecords = 
				allocateArrayRecords( arrayBlockId, value );
			for ( DynamicRecord valueRecord : arrayRecords )
			{
				valueRecord.setType( PropertyType.ARRAY.intValue() );
				record.addValueRecord( valueRecord );
			}
			record.setType( PropertyType.ARRAY );
		}
		else
		{
			throw new RuntimeException( "Unkown property type on: " + value );
		}
	}

	public Object getStringFor( PropertyRecord propRecord, 
		ReadFromBuffer buffer ) throws IOException
    {
		int recordToFind = (int) propRecord.getPropBlock();
		Iterator<DynamicRecord> records = 
			propRecord.getValueRecords().iterator();
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
					stringPropertyStore.makeHeavy( record, buffer );
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
				// TODO: make opti here, high chance next is right one
				records = propRecord.getValueRecords().iterator();
			}
		}
		StringBuffer buf = new StringBuffer();
		for ( char[] str : charList )
		{
			buf.append( str );
		}
		return buf.toString();
    }

	public Object getArrayFor( PropertyRecord propertyRecord, 
		ReadFromBuffer buffer ) throws IOException
    {
		return arrayPropertyStore.getArray( 
			(int) propertyRecord.getPropBlock() );
    }
	
	private void convertKeyToIndexStore() throws IOException
	{
		DynamicStringStore oldKeyStore = new DynamicStringStore( 
			getStorageFileName() + ".keys" );
		System.out.println( "Converting property keys to property indexes..." );
		PropertyIndexStore.createStore( getStorageFileName() + ".index" );
		propertyIndexStore = new PropertyIndexStore( getStorageFileName() + 
			".index", getConfig() );
		int maxId = getHighestPossibleIdInUse();
		Map<String,Integer> keyToIndex = new HashMap<String, Integer>();
		ByteBuffer buf = ByteBuffer.allocate( 9 );
		for ( int i = 0; i <= maxId; i++ )
		{
			int position = i * RECORD_SIZE; 
			getFileChannel().position( position );
			buf.clear();
			if ( getFileChannel().read( buf ) != 9 )
			{
				break; // we're done
			}
			buf.flip();
			if ( buf.get() == Record.IN_USE.byteValue() )
			{
				// convert to index
				buf.getInt();
				int oldKeyId = buf.getInt();
				String oldKey = getOldKeyStringFor( oldKeyId, oldKeyStore );
				int newIndexKeyId = -1;
				if ( !keyToIndex.containsKey( oldKey ) )
				{
					newIndexKeyId = createNewPropertyIndex( oldKey );
					keyToIndex.put( oldKey, newIndexKeyId );
				}
				else
				{
					newIndexKeyId = keyToIndex.get( oldKey );
				}
				buf.clear();
				buf.putInt( newIndexKeyId );
				buf.flip();
				getFileChannel().position( position + 5 );
				if ( getFileChannel().write( buf ) != 4 )
				{
					throw new IOException( "did not write 4 bytes..." );
				}
			}
		}
	}
	
	private int createNewPropertyIndex( String oldKey ) throws IOException
	{
		PropertyIndexRecord record = new PropertyIndexRecord( 
			propertyIndexStore.nextId() );
		record.setInUse( true );
		record.setCreated();
		int keyBlockId = propertyIndexStore.nextKeyBlockId();
		record.setKeyBlockId( keyBlockId );
		int length = oldKey.length();
		char[] chars = new char[length];
		oldKey.getChars( 0, length, chars, 0 );
		Collection<DynamicRecord> keyRecords = 
			propertyIndexStore.allocateKeyRecords( keyBlockId, chars );
		for ( DynamicRecord keyRecord : keyRecords )
		{
			record.addKeyRecord( keyRecord );
		}
		propertyIndexStore.updateRecord( record );
		return record.getId();
	}
	
	private String getOldKeyStringFor( int oldKeyId, DynamicStringStore 
		oldKeyStore ) throws IOException
    {
		Collection<DynamicRecord> allRecords = oldKeyStore.getRecords( oldKeyId, 
			null );
		Iterator<DynamicRecord> records = allRecords.iterator();
		List<byte[]> byteList = new LinkedList<byte[]>();
		int totalSize = 0;
		int recordToFind = oldKeyId;
		while ( recordToFind != Record.NO_NEXT_BLOCK.intValue() && 
			records.hasNext() )
		{
			DynamicRecord record = records.next();
			if ( record.inUse() && record.getId() == recordToFind )
			{
				if ( record.isLight() )
				{
					oldKeyStore.makeHeavy( record, null );
				}
				byteList.add( record.getData() );
				recordToFind = record.getNextBlock();
				records = allRecords.iterator();
			}
		}
		int totalLength = 0;
		for ( byte[] array : byteList )
		{
			totalLength += array.length;
		}
		byte[] byteArrayStr = new byte[totalLength];
		int position = 0;
		for ( byte[] array : byteList )
		{
			System.arraycopy( array, 0, byteArrayStr, position, array.length );
			position += array.length;
		}
		return new String( byteArrayStr );
    }
}
