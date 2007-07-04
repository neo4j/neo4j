package org.neo4j.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
		keyPropertyStore = new DynamicStringStore( 
			getStorageFileName() + ".keys", getConfig() );
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
		keyPropertyStore.close();
		keyPropertyStore = null;
		arrayPropertyStore.close();
		arrayPropertyStore = null;
	}
	
	@Override
	public void flush( int txIdentifier ) throws IOException
	{
		stringPropertyStore.flush( txIdentifier );
		keyPropertyStore.flush( txIdentifier );
		arrayPropertyStore.flush( txIdentifier );
		super.flush( txIdentifier );
	}
	
	@Override
	public void forget( int txIdentifier )
	{
		stringPropertyStore.forget( txIdentifier );
		keyPropertyStore.forget( txIdentifier );
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
		DynamicStringStore.createStore( fileName + ".keys", 
			KEY_STORE_BLOCK_SIZE );
		DynamicArrayStore.createStore( fileName + ".arrays", 
			STRING_STORE_BLOCK_SIZE );
	}
	
	public int nextKeyBlockId() throws IOException
	{
		return keyPropertyStore.nextBlockId();
	}

	private int nextStringBlockId() throws IOException
	{
		return stringPropertyStore.nextBlockId();
	}
	
	private int nextArrayBlockId() throws IOException
	{
		return arrayPropertyStore.nextBlockId();
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
		PropertyType propertyType = getEnumType( storeData.type() );
		switch ( propertyType )
		{
			case INT:
				return ( int ) storeData.propertyStoreBlockId();
			case STRING:
				return stringPropertyStore.getString(  
				 	( int ) storeData.propertyStoreBlockId() );
			case BOOL:
				if ( storeData.propertyStoreBlockId() == 1 )
				{
					return Boolean.valueOf( true );
				}
				return Boolean.valueOf( false );
			case DOUBLE:
				return new Double( Double.longBitsToDouble( 
						storeData.propertyStoreBlockId() ) );
			case FLOAT:
				return new Float( Float.intBitsToFloat( 
						( int ) storeData.propertyStoreBlockId() ) );
			case LONG:
				return storeData.propertyStoreBlockId();
			case BYTE:
				return (byte) storeData.propertyStoreBlockId();
			case CHAR:
				return (char) storeData.propertyStoreBlockId();
			case ARRAY:
				return arrayPropertyStore.getArray( 
					(int) storeData.propertyStoreBlockId() );
			default:
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
			else if ( record.getType() == PropertyType.ARRAY )
			{
				Collection<DynamicRecord> arrayRecords = 
					arrayPropertyStore.getRecords( 
						( int ) record.getPropBlock() );
				for ( DynamicRecord stringRecord : arrayRecords )
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
			case 8: return PropertyType.CHAR;
			case 9: return PropertyType.ARRAY;
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
		arrayPropertyStore.makeStoreOk();
		super.makeStoreOk();
	}
	
	@Override
	public void validate()
	{
		keyPropertyStore.validate();
		stringPropertyStore.validate();
		arrayPropertyStore.validate();
		super.validate();
	}

	public PropertyType getType( Object value )
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
		else if ( value instanceof Character )
		{
			return PropertyType.CHAR;
		}
		else if ( value.getClass().isArray() )
		{
			validatePrimitiveArray( value );
			return PropertyType.ARRAY;
		}
		throw new RuntimeException( "Unkown property type on: " + value );
	}
	
	private void validatePrimitiveArray( Object object )
	{
		if ( object instanceof int[] || object instanceof Integer[] )
		{
			return;
		}
		if ( object instanceof String[] )
		{
			return;
		}
		if ( object instanceof boolean[] || object instanceof Boolean[] )
		{
			return; 
		}
		if ( object instanceof double[] || object instanceof Double[] )
		{
			return;
		}
		if ( object instanceof float[] || object instanceof Float[] )
		{
			return;
		}
		if ( object instanceof long[] || object instanceof Long[] )
		{
			return;
		}
		if ( object instanceof byte[] || object instanceof Byte[] )
		{
			return;
		}
		if ( object instanceof char[] || object instanceof Character[] )
		{
			return;
		}
		throw new RuntimeException( object + " not a valid array type." );
	}

	public Collection<DynamicRecord> allocateKeyRecords( int keyBlockId, 
		byte[] bytes ) throws IOException
	{
		return keyPropertyStore.allocateRecords( keyBlockId, bytes );
	}

	private Collection<DynamicRecord> allocateStringRecords( int valueBlockId, 
		byte[] bytes ) throws IOException
	{
		return stringPropertyStore.allocateRecords( valueBlockId, bytes );
	}
	
	private Collection<DynamicRecord> allocateArrayRecords( int valueBlockId, 
		Object array ) throws IOException
	{
		return arrayPropertyStore.allocateRecords( valueBlockId, array );
	}

	public void encodeValue( PropertyRecord record, 
		Object value ) throws IOException
	{
		PropertyType type = record.getType();
		switch ( type )
		{
			case STRING:
				int stringBlockId = nextStringBlockId();
				record.setPropBlock( stringBlockId );
				Collection<DynamicRecord> valueRecords = 
					allocateStringRecords( stringBlockId, ( ( String ) 
						value ).getBytes() );
				for ( DynamicRecord valueRecord : valueRecords )
				{
					record.addValueRecord( valueRecord );
				}
				break;
			case INT:
				record.setPropBlock( ( ( Integer ) value ).intValue() );
				break;
			case BOOL:
				record.setPropBlock(  
					( ( ( Boolean ) value ).booleanValue() ? 1 : 0 ) );
				break;
			case DOUBLE:
				record.setPropBlock( Double.doubleToRawLongBits( 
					( ( Double ) value ).doubleValue() ) );
				break;
			case FLOAT:
				record.setPropBlock(  Float.floatToRawIntBits( 
					( ( Float ) value ).floatValue() ) );
				break;
			case LONG:
				record.setPropBlock( ( ( Long ) value ).longValue() );
				break;
			case BYTE:
				record.setPropBlock( (( Byte) value ).byteValue() );
				break;
			case CHAR:
				record.setPropBlock( (( Character) value ).charValue() );
				break;
			case ARRAY:
				int arrayBlockId = nextArrayBlockId();
				record.setPropBlock( arrayBlockId );
				Collection<DynamicRecord> arrayRecords = 
					allocateArrayRecords( arrayBlockId, value );
				for ( DynamicRecord valueRecord : arrayRecords )
				{
					record.addValueRecord( valueRecord );
				}
				break;
			default:
				throw new RuntimeException( "Unkown property type: " + type );
		}
	}	
}
