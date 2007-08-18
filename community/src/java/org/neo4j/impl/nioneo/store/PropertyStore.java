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
 * Implementation of the property store. This implementation has two dynamic 
 * stores. One used to store keys and another for string property values.
 */
public class PropertyStore extends AbstractStore implements Store
{
	// store version, each store ends with this string (byte encoded)
	private static final String VERSION = "PropertyStore v0.9.2";
	 
	// record header size
	// in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
	// prev_prop_id(int)+next_prop_id(int)
	private static final int RECORD_SIZE = 25;
	
	private static final int STRING_STORE_BLOCK_SIZE = 30;
	
//	private LruCache<Integer,PropertyRecord> cache = 
//		new LruCache<Integer,PropertyRecord>( "PropertyRecordCache", 4000 );

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
	
//	public int nextKeyBlockId() throws IOException
//	{
//		return keyPropertyStore.nextBlockId();
//	}

	private int nextStringBlockId() throws IOException
	{
		return stringPropertyStore.nextBlockId();
	}
	
	private int nextArrayBlockId() throws IOException
	{
		return arrayPropertyStore.nextBlockId();
	}
	
	public PropertyIndexStore getIndexStore()
	{
		return propertyIndexStore;
	}
	
//	public Object getPropertyValue( int id ) throws IOException
//	{
//		// update statistics
//		PersistenceWindow window = acquireWindow( id, OperationType.READ );
//		PropertyStoreData storeData = null;
//		try
//		{
//			storeData = getProperty( id, window.getBuffer() );
//		}
//		finally
//		{
//			releaseWindow( window );
//		}
//		String key = null;
//		PropertyType propertyType = getEnumType( storeData.type() );
//		switch ( propertyType )
//		{
//			case INT:
//				return ( int ) storeData.propertyStoreBlockId();
//			case STRING:
//				return stringPropertyStore.getString(  
//				 	( int ) storeData.propertyStoreBlockId() );
//			case BOOL:
//				if ( storeData.propertyStoreBlockId() == 1 )
//				{
//					return Boolean.valueOf( true );
//				}
//				return Boolean.valueOf( false );
//			case DOUBLE:
//				return new Double( Double.longBitsToDouble( 
//						storeData.propertyStoreBlockId() ) );
//			case FLOAT:
//				return new Float( Float.intBitsToFloat( 
//						( int ) storeData.propertyStoreBlockId() ) );
//			case LONG:
//				return storeData.propertyStoreBlockId();
//			case BYTE:
//				return (byte) storeData.propertyStoreBlockId();
//			case CHAR:
//				return (char) storeData.propertyStoreBlockId();
//			case ARRAY:
//				return arrayPropertyStore.getArray( 
//					(int) storeData.propertyStoreBlockId() );
//			default:
//				throw new IOException( "Unkown type[" + storeData.type() + 
//					"] on property[" + id + "] key[" + key + "]" );
//		}
//	}
	
//	private PropertyData getLightProperty( int id ) throws IOException
//	{
//		PersistenceWindow window = acquireWindow( id, OperationType.READ );
//		PropertyStoreData storeData = null;
//		try
//		{
//			storeData = getProperty( id, window.getBuffer() );
//		}
//		finally
//		{
//			releaseWindow( window );
//		}
////		try
////		{
////			key = propertyIndexStore.getString( storeData.keyStoreBlockId() );
////		}
////		catch ( IOException e )
////		{
////			Logger.getLogger( PropertyStore.class.getName() ).severe( 
////				"Failed to get key string on property[" + 
////				id + "] type= " + storeData.type() + " keyStoreBlockId= " + 
////				storeData.keyStoreBlockId() + " propertyStoreBlockId= " + 
////				storeData.propertyStoreBlockId() + " previousPropertyId= " + 
////				storeData.previousPropertyId() + " nextPropertyId= " + 
////				storeData.nextPropertyId() );
////			throw e;
////		}
//		// check statistics if low
//		return new PropertyData( id, storeData.keyIndexId(), null, 
//			storeData.nextPropertyId() );
//		// if high load property
//	}
	
//	public PropertyData[] getProperties( int startPropertyId )
//		throws IOException
//	{
//		ArrayList<PropertyData> propertyDataList = 
//			new ArrayList<PropertyData>();
//		int nextPropertyId = startPropertyId;
//		while ( nextPropertyId != Record.NO_NEXT_PROPERTY.intValue() ) 
//		{
//			PropertyData data = getLightProperty( nextPropertyId );
//			propertyDataList.add( data );
//			nextPropertyId = data.nextPropertyId();
//		}
//		return propertyDataList.toArray( 
//			new PropertyData[ propertyDataList.size() ] );
//	}
	
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
//			for ( DynamicRecord keyRecord : record.getKeyRecords() )
//			{
//				keyPropertyStore.updateRecord( keyRecord );
//			}
			if ( record.getType() == PropertyType.STRING )
			{
				for ( DynamicRecord valueRecord : record.getValueRecords() )
				{
					stringPropertyStore.updateRecord( valueRecord );
				}
			}
			else if ( record.getType() == PropertyType.ARRAY )
			{
				for ( DynamicRecord valueRecord : record.getValueRecords() )
				{
					arrayPropertyStore.updateRecord( valueRecord );
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
//		getFileChannel().force( false );
//		PropertyRecord check = getLightRecord( record.getId() );
//		ByteBuffer buf = ByteBuffer.allocate( 25 );
//		long oldPos = record.getFromChannel().position();
//		record.getFromChannel().position( record.getTransferStartPosition() );
//		record.getFromChannel().read( buf );
//		buf.flip();
//		System.out.print( "id=" + record.getId() + " " );
//		System.out.println( "inUse=" + buf.get() + " type=" + buf.getInt() + 
//			" key=" + buf.getInt() + " prop=" + buf.getLong() + " prev=" + 
//			buf.getInt() + " next=" + buf.getInt() );
//		record.getFromChannel().position( oldPos );
//		getFileChannel().position( record.getId() * getRecordSize() );
//		buf.clear();
//		getFileChannel().read( buf );
//		buf.flip();
//		System.out.println( "[inUse=" + buf.get() + " type=" + buf.getInt() + 
//			" key=" + buf.getInt() + " prop=" + buf.getLong() + " prev=" + 
//			buf.getInt() + " next=" + buf.getInt() );
//		assert check.getType() == record.getType();
//		assert check.getKeyIndexId() == record.getKeyIndexId();
//		assert check.getPropBlock() == record.getPropBlock();
//		assert check.getPrevProp() == record.getPrevProp();
//		assert check.getNextProp() == record.getNextProp();
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
//				.putInt( 0 ).putInt( 
//				Record.NO_NEXT_BLOCK.intValue() ).putLong( 
//				Record.NO_NEXT_BLOCK.intValue() ).putInt( 
//				Record.NO_PREVIOUS_PROPERTY.intValue() ).putInt( 
//				Record.NO_NEXT_PROPERTY.intValue() );
			if ( !isInRecoveryMode() )
			{
				freeId( id );
			}
		}
	}
	
	public PropertyRecord getLightRecord( int id, ReadFromBuffer buffer ) 
		throws IOException
	{
		PropertyRecord record; // = cache.get( id );
//		if ( record != null )
//		{
//			assert record.inUse();
//			return record;
//		}
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
//			cache.add( id, record );
			return record;
		}
		PersistenceWindow window = acquireWindow( id, OperationType.READ );
		try
		{
			record = getRecord( id, window.getBuffer() );
			record.setIsLight( true );
//			cache.add( id, record );
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
//		Collection<DynamicRecord> keyRecords = 
//			keyPropertyStore.getLightRecords( record.getKeyBlock() );
//		for ( DynamicRecord keyRecord : keyRecords )
//		{
//			record.addKeyRecord( keyRecord );
//		}
		if ( record.getType() == PropertyType.STRING )
		{
			Collection<DynamicRecord> stringRecords = 
				stringPropertyStore.getLightRecords( 
					( int ) record.getPropBlock(), buffer );
			for ( DynamicRecord stringRecord : stringRecords )
			{
				record.addValueRecord( stringRecord );
			}
		}
		else if ( record.getType() == PropertyType.ARRAY )
		{
			Collection<DynamicRecord> arrayRecords = 
				arrayPropertyStore.getLightRecords( 
					( int ) record.getPropBlock(), buffer );
			for ( DynamicRecord stringRecord : arrayRecords )
			{
				record.addValueRecord( stringRecord );
			}
		}
	}
	
	public PropertyRecord getRecord( int id, ReadFromBuffer buffer ) 
		throws IOException
	{
		PropertyRecord record; // = cache.get( id );
//		if ( record != null )
//		{
//			assert record.inUse();
//		}
		/*else*/ if ( buffer != null && !hasWindow( id ) )
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
			// cache.add( id, record );
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
		//		Collection<DynamicRecord> keyRecords = 
		//		keyPropertyStore.getLightRecords( record.getKeyBlock() );
		//	for ( DynamicRecord keyRecord : keyRecords )
		//	{
		//		record.addKeyRecord( keyRecord );
		//	}
		if ( record.getType() == PropertyType.STRING )
		{
			Collection<DynamicRecord> stringRecords = 
				stringPropertyStore.getLightRecords( 
					(int) record.getPropBlock(), buffer );
			record.setIsLight( false );
			for ( DynamicRecord stringRecord : stringRecords )
			{
				record.addValueRecord( stringRecord );
			}
		}
		else if ( record.getType() == PropertyType.ARRAY )
		{
			Collection<DynamicRecord> arrayRecords = 
				arrayPropertyStore.getLightRecords( 
					(int) record.getPropBlock(), buffer );
			record.setIsLight( false );
			for ( DynamicRecord stringRecord : arrayRecords )
			{
				record.addValueRecord( stringRecord );
			}
		}
		return record;
	}
	
//	private static class PropertyStoreData
//	{
//		private int type;
//		private int keyIndexId;
//		private long propertyStoreBlockId;
////		private int previousPropertyId;
////		private int nextPropertyId;
//		
//		PropertyStoreData( int type, int keyIndexId, long prop )//, 
//			//int previousPropertyId, int nextPropertyId )
//		{
//			this.type = type;
//			this.keyIndexId = keyIndexId;
//			this.propertyStoreBlockId = prop;
////			this.previousPropertyId = previousPropertyId;
////			this.nextPropertyId = nextPropertyId;
//		}
//		
//		public int type()
//		{
//			return type;
//		}
//		
//		public int keyIndexId()
//		{
//			return keyIndexId;
//		}
//		
//		public long propertyStoreBlockId()
//		{
//			return propertyStoreBlockId;
//		}
//		
////		public int previousPropertyId()
////		{
////			return previousPropertyId;
////		}
////
////		public int nextPropertyId()
////		{
////			return nextPropertyId;
////		}
//	}
	
	private PropertyRecord getRecord( int id, Buffer buffer ) 
		throws IOException
	{
		int offset = ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		if ( buffer.get() != Record.IN_USE.byteValue() )
		{
			throw new IOException( "Record[" + id + "] not in use" );
		}
//		PropertyRecord record = new PropertyRecord( id, 
//			getEnumType( buffer.getInt() ) );
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

//	private PropertyStoreData getProperty( int id, Buffer buffer ) 
//		throws IOException 
//	{
//		int offset = ( id - buffer.position() ) * getRecordSize();
//		buffer.setOffset( offset );
//		if ( buffer.get() != Record.IN_USE.byteValue() )
//		{
//			throw new IOException( "Record[" + id + "] not in use" );
//		}
//		return new PropertyStoreData( buffer.getInt(), buffer.getInt(), 
//			buffer.getLong(), buffer.getInt(), buffer.getInt() );
//	}
	
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

//	public PropertyType getType( Object value )
//	{
//		if ( value instanceof String )
//		{
//			return PropertyType.STRING;
//		}
//		else if ( value instanceof Integer )
//		{
//			return PropertyType.INT;
//		}
//		else if ( value instanceof Boolean )
//		{
//			return PropertyType.BOOL;
//		}
//		else if ( value instanceof Float )
//		{
//			return PropertyType.FLOAT;
//		}
//		else if ( value instanceof Long )
//		{
//			return PropertyType.LONG;
//		}
//		else if ( value instanceof Double )
//		{
//			return PropertyType.DOUBLE;
//		}
//		else if ( value instanceof Byte )
//		{
//			return PropertyType.BYTE;
//		}
//		else if ( value instanceof Character )
//		{
//			return PropertyType.CHAR;
//		}
//		else if ( value.getClass().isArray() )
//		{
//			validatePrimitiveArray( value );
//			return PropertyType.ARRAY;
//		}
//		throw new RuntimeException( "Unkown property type on: " + value );
//	}
	
//	private void validatePrimitiveArray( Object object )
//	{
//		if ( object instanceof int[] || object instanceof Integer[] )
//		{
//			return;
//		}
//		if ( object instanceof String[] )
//		{
//			return;
//		}
//		if ( object instanceof boolean[] || object instanceof Boolean[] )
//		{
//			return; 
//		}
//		if ( object instanceof double[] || object instanceof Double[] )
//		{
//			return;
//		}
//		if ( object instanceof float[] || object instanceof Float[] )
//		{
//			return;
//		}
//		if ( object instanceof long[] || object instanceof Long[] )
//		{
//			return;
//		}
//		if ( object instanceof byte[] || object instanceof Byte[] )
//		{
//			return;
//		}
//		if ( object instanceof char[] || object instanceof Character[] )
//		{
//			return;
//		}
//		throw new RuntimeException( object + " not a valid array type." );
//	}

//	public Collection<DynamicRecord> allocateKeyRecords( int keyBlockId, 
//		byte[] bytes ) throws IOException
//	{
//		return keyPropertyStore.allocateRecords( keyBlockId, bytes );
//	}
	
//	public Collection<DynamicRecord> allocateKeyRecords( int keyBlockId, 
//		char[] chars ) throws IOException
//	{
//		return keyPropertyStore.allocateRecords( keyBlockId, chars );
//	}

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
				record.addValueRecord( valueRecord );
			}
			record.setType( PropertyType.ARRAY );
		}
		else
		{
			throw new RuntimeException( "Unkown property type on: " + value );
		}
//		PropertyType type = record.getType();
//		switch ( type )
//		{
//			case STRING:
//				int stringBlockId = nextStringBlockId();
//				record.setPropBlock( stringBlockId );
//				String string = (String) value;
//				int length = string.length();
//				char[] chars = new char[length];
//				string.getChars( 0, length, chars, 0 );
//				Collection<DynamicRecord> valueRecords = 
//					allocateStringRecords( stringBlockId, chars );
//				for ( DynamicRecord valueRecord : valueRecords )
//				{
//					record.addValueRecord( valueRecord );
//				}
//				break;
//			case INT:
//				record.setPropBlock( ( ( Integer ) value ).intValue() );
//				break;
//			case BOOL:
//				record.setPropBlock(  
//					( ( ( Boolean ) value ).booleanValue() ? 1 : 0 ) );
//				break;
//			case DOUBLE:
//				record.setPropBlock( Double.doubleToRawLongBits( 
//					( ( Double ) value ).doubleValue() ) );
//				break;
//			case FLOAT:
//				record.setPropBlock(  Float.floatToRawIntBits( 
//					( ( Float ) value ).floatValue() ) );
//				break;
//			case LONG:
//				record.setPropBlock( ( ( Long ) value ).longValue() );
//				break;
//			case BYTE:
//				record.setPropBlock( (( Byte) value ).byteValue() );
//				break;
//			case CHAR:
//				record.setPropBlock( (( Character) value ).charValue() );
//				break;
//			case ARRAY:
//				int arrayBlockId = nextArrayBlockId();
//				record.setPropBlock( arrayBlockId );
//				Collection<DynamicRecord> arrayRecords = 
//					allocateArrayRecords( arrayBlockId, value );
//				for ( DynamicRecord valueRecord : arrayRecords )
//				{
//					record.addValueRecord( valueRecord );
//				}
//				break;
//			default:
//				throw new RuntimeException( "Unkown property type: " + type );
//		}
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
				ByteBuffer buf = ByteBuffer.wrap( record.getData() );
				char[] chars = new char[ record.getData().length / 2 ];
				totalSize += chars.length;
				buf.asCharBuffer().get( chars );
				charList.add( chars );
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

//	public void purge( int id )
//    {
//		cache.remove( id );
//    }
}
