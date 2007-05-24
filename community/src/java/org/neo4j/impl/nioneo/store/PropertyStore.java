package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;


/**
 * Implementation of the property store. This implementation has two dynamic 
 * stores. One used to store keys and another for string property values.
 */
public class PropertyStore extends AbstractStore implements Store
{
	// store version, each store ends with this string (byte encoded)
	private static final String VERSION = "PropertyStore v0.9.1";
	 
	// record header size
	// in_use(byte)+type(int)+key_blockId(int)+prop_blockId(long)+
	// prev_prop_id(int)+next_prop_id(int)
	private static final int RECORD_SIZE = 25;
	
	private static final int STRING_STORE_BLOCK_SIZE = 30;
	private static final int KEY_STORE_BLOCK_SIZE = 30;
	 

	private DynamicStringStore stringPropertyStore;
	private DynamicStringStore keyPropertyStore;
	
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
		keyPropertyStore = new DynamicStringStore( 
			getStorageFileName() + ".keys", getConfig() );
	}
	
	@Override
	protected void closeStorage() throws IOException
	{
		stringPropertyStore.close();
		stringPropertyStore = null;
		keyPropertyStore.close();
		keyPropertyStore = null;
	}
	
	@Override
	public void flush( int txIdentifier ) throws IOException
	{
		stringPropertyStore.flush( txIdentifier );
		keyPropertyStore.flush( txIdentifier );
		super.flush( txIdentifier );
	}
	
	@Override
	public void forget( int txIdentifier )
	{
		stringPropertyStore.forget( txIdentifier );
		keyPropertyStore.forget( txIdentifier );
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
		DynamicStringStore.createStore( fileName + ".keys", 
			KEY_STORE_BLOCK_SIZE );
	}
	
	public int nextKeyBlockId() throws IOException
	{
		return keyPropertyStore.nextBlockId();
	}

	public int nextValueBlockId() throws IOException
	{
		return stringPropertyStore.nextBlockId();
	}
	
	public void freeKeyBlockId( int blockId ) throws IOException
	{
		keyPropertyStore.freeBlockId( blockId );
	}
	
	public void freeValueBlockId( int blockId ) throws IOException
	{
		stringPropertyStore.freeBlockId( blockId );
	}

	public Object getPropertyValue( int id ) throws IOException
	{
		// update statistics
		PersistenceWindow window = acquireWindow( id, OperationType.READ );
		PropertyStoreData storeData = null;
		try
		{
			storeData = getProperty( id, window.getBuffer() );
		}
		finally
		{
			releaseWindow( window );
		}
		String key = null;
		if ( storeData.type() == PropertyType.INT.intValue() )
		{
			return new Integer( ( int ) storeData.propertyStoreBlockId() );
		}
		else if ( storeData.type() == PropertyType.STRING.intValue() )
		{
			return stringPropertyStore.getString(  
			 	( int ) storeData.propertyStoreBlockId() );
		}
		else if ( storeData.type() == PropertyType.BOOL.intValue() )
		{
			Boolean value = null;
			if ( storeData.propertyStoreBlockId() == 1 )
			{
				value = new Boolean( true );
			}
			else
			{
				value = new Boolean( false );
			}
			return value; 
		}
		else if ( storeData.type() == PropertyType.DOUBLE.intValue() )
		{
			return new Double( Double.longBitsToDouble( 
					storeData.propertyStoreBlockId() ) );
		}
		else if ( storeData.type() == PropertyType.FLOAT.intValue() )
		{
			return new Float( Float.intBitsToFloat( 
					( int ) storeData.propertyStoreBlockId() ) );
		}
		else if ( storeData.type() == PropertyType.LONG.intValue() )
		{
			return new Long( storeData.propertyStoreBlockId() );
		}
		else if ( storeData.type() == PropertyType.BYTE.intValue() )
		{
			return new Byte( (byte) storeData.propertyStoreBlockId() );
		}
		else
		{
			throw new IOException( "Unkown type[" + storeData.type() + 
				"] on property[" + id + "] key[" + key + "]" );
		}
	}
	
	private PropertyData getLightProperty( int id ) throws IOException
	{
		PersistenceWindow window = acquireWindow( id, OperationType.READ );
		PropertyStoreData storeData = null;
		try
		{
			storeData = getProperty( id, window.getBuffer() );
		}
		finally
		{
			releaseWindow( window );
		}
		String key = null;
		try
		{
			key = keyPropertyStore.getString( storeData.keyStoreBlockId() );
		}
		catch ( IOException e )
		{
			Logger.getLogger( PropertyStore.class.getName() ).severe( 
				"Failed to get key string on property[" + 
				id + "] type= " + storeData.type() + " keyStoreBlockId= " + 
				storeData.keyStoreBlockId() + " propertyStoreBlockId= " + 
				storeData.propertyStoreBlockId() + " previousPropertyId= " + 
				storeData.previousPropertyId() + " nextPropertyId= " + 
				storeData.nextPropertyId() );
			throw e;
		}
		// check statistics if low
		return new PropertyData( id, key, null, storeData.nextPropertyId() );
		// if high load property
	}
	
	public PropertyData[] getProperties( int startPropertyId )
		throws IOException
	{
		ArrayList<PropertyData> propertyDataList = 
			new ArrayList<PropertyData>();
		int nextPropertyId = startPropertyId;
		while ( nextPropertyId != Record.NO_NEXT_PROPERTY.intValue() ) 
		{
			PropertyData data = getLightProperty( nextPropertyId );
			propertyDataList.add( data );
			nextPropertyId = data.nextPropertyId();
		}
		return propertyDataList.toArray( 
			new PropertyData[ propertyDataList.size() ] );
	}
	
	public void updateRecord( PropertyRecord record ) throws IOException
	{
		PersistenceWindow window = acquireWindow( record.getId(), 
				OperationType.WRITE );
		try
		{
			updateRecord( record, window.getBuffer() );
			for ( DynamicRecord keyRecord : record.getKeyRecords() )
			{
				keyPropertyStore.updateRecord( keyRecord );
			}
			for ( DynamicRecord valueRecord : record.getValueRecords() )
			{
				stringPropertyStore.updateRecord( valueRecord );
			}
		}
		finally 
		{
			releaseWindow( window );
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
				record.getType().intValue() ).putInt( record.getKeyBlock() 
				).putLong( record.getPropBlock() ).putInt( 
				record.getPrevProp() ).putInt( record.getNextProp() );
		}
		else
		{
			buffer.put( Record.NOT_IN_USE.byteValue() ).putInt( 0 ).putInt( 
				Record.NO_NEXT_BLOCK.intValue() ).putLong( 
				Record.NO_NEXT_BLOCK.intValue() ).putInt( 
				Record.NO_PREVIOUS_PROPERTY.intValue() ).putInt( 
				Record.NO_NEXT_PROPERTY.intValue() );
			if ( !isInRecoveryMode() )
			{
				freeId( id );
			}
		}
	}
	
	public PropertyRecord getRecord( int id ) throws IOException
	{
		PersistenceWindow window = acquireWindow( id, OperationType.READ );
		try
		{
			PropertyRecord record = getRecord( id, window.getBuffer() );
			Collection<DynamicRecord> keyRecords = 
				keyPropertyStore.getRecords( record.getKeyBlock() );
			for ( DynamicRecord keyRecord : keyRecords )
			{
				record.addKeyRecord( keyRecord );
			}
			if ( record.getType() == PropertyType.STRING )
			{
				Collection<DynamicRecord> stringRecords = 
					stringPropertyStore.getRecords( 
						( int ) record.getPropBlock() );
				for ( DynamicRecord stringRecord : stringRecords )
				{
					record.addValueRecord( stringRecord );
				}
			}
			return record;
		}
		finally 
		{
			releaseWindow( window );
		}
	}
	
	private static class PropertyStoreData
	{
		private int type;
		private int keyStoreBlockId;
		private long propertyStoreBlockId;
		private int previousPropertyId;
		private int nextPropertyId;
		
		PropertyStoreData( int type, int key, long prop, 
			int previousPropertyId, int nextPropertyId )
		{
			this.type = type;
			this.keyStoreBlockId = key;
			this.propertyStoreBlockId = prop;
			this.previousPropertyId = previousPropertyId;
			this.nextPropertyId = nextPropertyId;
		}
		
		public int type()
		{
			return type;
		}
		
		public int keyStoreBlockId()
		{
			return keyStoreBlockId;
		}
		
		public long propertyStoreBlockId()
		{
			return propertyStoreBlockId;
		}
		
		public int previousPropertyId()
		{
			return previousPropertyId;
		}

		public int nextPropertyId()
		{
			return nextPropertyId;
		}
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
		PropertyRecord record = new PropertyRecord( id, 
			getEnumType( buffer.getInt() ) );
		record.setInUse( true );
		record.setKeyBlock( buffer.getInt() );
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
			default: throw new IOException( "Unkown enum type:" +
				type );
		}
	}

	private PropertyStoreData getProperty( int id, Buffer buffer ) 
		throws IOException 
	{
		int offset = ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		if ( buffer.get() != Record.IN_USE.byteValue() )
		{
			throw new IOException( "Record[" + id + "] not in use" );
		}
		return new PropertyStoreData( buffer.getInt(), buffer.getInt(), 
			buffer.getLong(), buffer.getInt(), buffer.getInt() );
	}
	
	@Override
	public void makeStoreOk() throws IOException
	{
		keyPropertyStore.makeStoreOk();
		stringPropertyStore.makeStoreOk();
		super.makeStoreOk();
	}
	
	@Override
	public void validate()
	{
		keyPropertyStore.validate();
		stringPropertyStore.validate();
		super.validate();
	}

	public PropertyType getType( Object value ) throws IOException
	{
		if ( value instanceof String )
		{
			return PropertyType.STRING;
		}
		else if ( value instanceof Integer )
		{
			return PropertyType.INT;
		}
		else if ( value instanceof Boolean )
		{
			return PropertyType.BOOL;
		}
		else if ( value instanceof Float )
		{
			return PropertyType.FLOAT;
		}
		else if ( value instanceof Long )
		{
			return PropertyType.LONG;
		}
		else if ( value instanceof Double )
		{
			return PropertyType.DOUBLE;
		}
		else if ( value instanceof Byte )
		{
			return PropertyType.BYTE;
		}
		throw new RuntimeException( "Unkown property type on: " + value );
	}

	public Collection<DynamicRecord> allocateKeyRecords( int keyBlockId, 
		byte[] bytes ) throws IOException
	{
		return keyPropertyStore.allocateRecords( keyBlockId, bytes );
	}

	public Collection<DynamicRecord> allocateValueRecords( int valueBlockId, 
		byte[] bytes ) throws IOException
	{
		return stringPropertyStore.allocateRecords( valueBlockId, bytes );
	}

	public Collection<Integer> encodeValue( PropertyRecord record, 
		Object value ) throws IOException
	{
		// TODO loose return
		List<Integer> ids = new LinkedList<Integer>();
		PropertyType type = record.getType();
		if ( type == PropertyType.STRING )
		{
			int valueBlockId = nextValueBlockId();
			record.setPropBlock( valueBlockId );
			Collection<DynamicRecord> valueRecords = 
				allocateValueRecords( valueBlockId, ( ( String ) 
					value ).getBytes() );
			for ( DynamicRecord valueRecord : valueRecords )
			{
				record.addValueRecord( valueRecord );
				ids.add( valueRecord.getId() );
			}
		}
		else if ( type == PropertyType.INT )
		{
			// store int value in the propertyStoreBlockId instead
			record.setPropBlock( ( ( Integer ) value ).intValue() );
		}
		else if ( type == PropertyType.BOOL )
		{
			record.setPropBlock(  
				( ( ( Boolean ) value ).booleanValue() ? 1 : 0 ) );
		}
		else if ( type == PropertyType.DOUBLE )
		{
			record.setPropBlock( Double.doubleToRawLongBits( 
				( ( Double ) value ).doubleValue() ) );
		}
		else if ( type == PropertyType.FLOAT )
		{
			record.setPropBlock(  Float.floatToRawIntBits( 
				( ( Float ) value ).floatValue() ) );
		}
		else if ( type == PropertyType.LONG )
		{
			record.setPropBlock( ( ( Long ) value ).longValue() );
		}
		else if ( type == PropertyType.BYTE )
		{
			record.setPropBlock( (( Byte) value ).byteValue() );
		}
		else
		{
			throw new RuntimeException( "Unkown property type: " + type );
		}
		return ids;
	}
}
