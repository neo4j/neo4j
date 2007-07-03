package org.neo4j.impl.nioneo.store;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

/**
 * Dynamic store that stores strings. 
 */
class DynamicArrayStore extends AbstractDynamicStore
{
	// store version, each store ends with this string (byte encoded)
	private static final String VERSION = "ArrayPropertyStore v0.9";
	
	private static enum ArrayType
	{
		ILLEGAL(0),
		INT(1), 
		STRING(2),
		BOOL(3), 
		DOUBLE(4),
		FLOAT(5), 
		LONG(6), 	
		BYTE(7),
		CHAR(8);
		
		private int type;
		
		ArrayType( int type )
		{
			this.type = type;
		}

		public byte byteValue()
		{
			return (byte) type;
		}
	}
	
	public DynamicArrayStore( String fileName, Map config ) 
		throws IOException
	{
		super( fileName, config );
	}

	public DynamicArrayStore( String fileName ) 
		throws IOException
	{
		super( fileName );
	}
	
	public String getTypeAndVersionDescriptor()
	{
		return VERSION;
	}
	
	public static void createStore( String fileName, 
		int blockSize ) throws IOException
	{
		createEmptyStore( fileName, blockSize, VERSION );
	}
	
	private Collection<DynamicRecord> allocateFromInt( int startBlock, 
		int[] array ) throws IOException
	{
		int size = array.length * 4 + 1;
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.INT.byteValue() );
		for ( int i : array )
		{
			buf.putInt( i );
		}
		return allocateRecords( startBlock, buf.array() );
	}

	private Collection<DynamicRecord> allocateFromInt( int startBlock, 
		Integer[] array ) throws IOException
	{
		int size = array.length * 4 + 1;
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.INT.byteValue() );
		for ( int i : array )
		{
			buf.putInt( i );
		}
		return allocateRecords( startBlock, buf.array() );
	}
	
	private Collection<DynamicRecord> allocateFromString( int startBlock, 
		String[] array ) throws IOException
	{
		int size = 5; 
		for ( String str : array )
		{
			size += 4 + str.getBytes().length;
		}
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.STRING.byteValue() );
		buf.putInt( array.length );
		for ( String str : array )
		{
			buf.putInt( str.getBytes().length );
			buf.put( str.getBytes() );
		}
		return allocateRecords( startBlock, buf.array() );
	}
	
	private Collection<DynamicRecord> allocateFromBool( int startBlock, 
		boolean[] array ) throws IOException
	{
		int size = 5 + array.length / 8;
		if ( array.length % 8 > 0 )
		{
			size++;
		}
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.BOOL.byteValue() );
		buf.putInt( array.length );
		byte currentValue = 0;
		int byteItr = 0;
		for ( boolean b : array )
		{
			if ( b )
			{
				currentValue += 1 << byteItr;
			}
			byteItr++;
			if ( byteItr == 8 )
			{
				buf.put( currentValue );
				byteItr = 0;
				currentValue = 0;
			}
		}
		if ( byteItr != 0 )
		{
			buf.put( currentValue );
		}
		return allocateRecords( startBlock, buf.array() );
	}
	
	private Collection<DynamicRecord> allocateFromBool( int startBlock, 
		Boolean[] array ) throws IOException
	{
		int size = 5 + array.length / 8;
		if ( array.length % 8 > 0 )
		{
			size++;
		}
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.BOOL.byteValue() );
		buf.putInt( array.length );
		byte currentValue = 0;
		int byteItr = 0;
		for ( Boolean b : array )
		{
			if ( b )
			{
				currentValue += 1 << byteItr;
			}
			byteItr++;
			if ( byteItr == 8 )
			{
				buf.put( currentValue );
				byteItr = 0;
				currentValue = 0;
			}
		}
		if ( byteItr != 0 )
		{
			buf.put( currentValue );
		}
		return allocateRecords( startBlock, buf.array() );
	}
	
	private Collection<DynamicRecord> allocateFromDouble( int startBlock, 
		double[] array ) throws IOException
	{
		int size = array.length * 8 + 1;
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.DOUBLE.byteValue() );
		for ( double d : array )
		{
			buf.putDouble( d );
		}
		return allocateRecords( startBlock, buf.array() );
	}

	private Collection<DynamicRecord> allocateFromDouble( int startBlock, 
		Double[] array ) throws IOException
	{
		int size = array.length * 8 + 1;
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.DOUBLE.byteValue() );
		for ( double d : array )
		{
			buf.putDouble( d );
		}
		return allocateRecords( startBlock, buf.array() );
	}
	
	private Collection<DynamicRecord> allocateFromFloat( int startBlock, 
		float[] array ) throws IOException
	{
		int size = array.length * 4 + 1;
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.FLOAT.byteValue() );
		for ( float f : array )
		{
			buf.putFloat( f );
		}
		return allocateRecords( startBlock, buf.array() );
	}

	private Collection<DynamicRecord> allocateFromFloat( int startBlock, 
		Float[] array ) throws IOException
	{
		int size = array.length * 4 + 1;
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.FLOAT.byteValue() );
		for ( float f : array )
		{
			buf.putFloat( f );
		}
		return allocateRecords( startBlock, buf.array() );
	}
	
	private Collection<DynamicRecord> allocateFromLong( int startBlock, 
		long[] array ) throws IOException
	{
		int size = array.length * 8 + 1;
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.LONG.byteValue() );
		for ( long l : array )
		{
			buf.putLong( l );
		}
		return allocateRecords( startBlock, buf.array() );
	}

	private Collection<DynamicRecord> allocateFromLong( int startBlock, 
		Long[] array ) throws IOException
	{
		int size = array.length * 8 + 1;
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.LONG.byteValue() );
		for ( long l : array )
		{
			buf.putLong( l );
		}
		return allocateRecords( startBlock, buf.array() );
	}
	
	private Collection<DynamicRecord> allocateFromByte( int startBlock, 
		byte[] array ) throws IOException
	{
		int size = array.length + 1;
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.BYTE.byteValue() );
		buf.put( array );
		return allocateRecords( startBlock, buf.array() );
	}

	private Collection<DynamicRecord> allocateFromByte( int startBlock, 
		Byte[] array ) throws IOException
	{
		int size = array.length + 1;
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.BYTE.byteValue() );
		for ( byte b : array )
		{
			buf.put( b );
		}
		return allocateRecords( startBlock, buf.array() );
	}
	
	private Collection<DynamicRecord> allocateFromChar( int startBlock, 
		char[] array ) throws IOException
	{
		int size = array.length * 2 + 1;
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.CHAR.byteValue() );
		for ( char c : array )
		{
			buf.putChar( c );
		}
		return allocateRecords( startBlock, buf.array() );
	}

	private Collection<DynamicRecord> allocateFromChar( int startBlock, 
		Character[] array ) throws IOException
	{
		int size = array.length * 2 + 1;
		ByteBuffer buf = ByteBuffer.allocate( size );
		buf.put( ArrayType.CHAR.byteValue() );
		for ( char c : array )
		{
			buf.putChar( c );
		}
		return allocateRecords( startBlock, buf.array() );
	}
	
	public Collection<DynamicRecord> allocateRecords( int startBlock, 
		Object array ) throws IOException
	{
		if ( array instanceof int[] )
		{
			return allocateFromInt( startBlock, (int[]) array );
		}
		if ( array instanceof Integer[] )
		{
			return allocateFromInt( startBlock, (Integer[]) array );
		}
		if ( array instanceof String[] )
		{
			return allocateFromString( startBlock, (String[]) array );
		}
		if ( array instanceof boolean[] )
		{
			return allocateFromBool( startBlock, (boolean[]) array );
		}
		if ( array instanceof Boolean[] )
		{
			return allocateFromBool( startBlock, (Boolean[]) array );
		}
		if ( array instanceof double[] )
		{
			return allocateFromDouble( startBlock, (double[]) array );
		}
		if ( array instanceof Double[] )
		{
			return allocateFromDouble( startBlock, (Double[]) array );
		}
		if ( array instanceof float[] )
		{
			return allocateFromFloat( startBlock, (float[]) array );
		}
		if ( array instanceof Float[] )
		{
			return allocateFromFloat( startBlock, (Float[]) array );
		}
		if ( array instanceof long[] )
		{
			return allocateFromLong( startBlock, (long[]) array );
		}
		if ( array instanceof Long[] )
		{
			return allocateFromLong( startBlock, (Long[]) array );
		}
		if ( array instanceof byte[] )
		{
			return allocateFromByte( startBlock, (byte[]) array );
		}
		if ( array instanceof Byte[] )
		{
			return allocateFromByte( startBlock, (Byte[]) array );
		}
		if ( array instanceof char[] )
		{
			return allocateFromChar( startBlock, (char[]) array );
		}
		if ( array instanceof Character[] )
		{
			return allocateFromChar( startBlock, (Character[]) array );
		}
		throw new RuntimeException( array + " not a valid array type." );
	}
	
	public Object getArray( int blockId ) throws IOException
	{
		byte bArray[] = get( blockId );
		ByteBuffer buf = ByteBuffer.wrap( bArray );
		byte type = buf.get();
		if ( type == ArrayType.INT.byteValue() )
		{
			int size = ( bArray.length - 1 ) / 4;
			assert ( bArray.length - 1 ) % 4 == 0;
			int[] array = new int[size];
			for ( int i = 0; i < size; i++ )
			{
				array[i] = buf.getInt();
			}
			return array;
		}
		if ( type == ArrayType.STRING.byteValue() )
		{
			String[] array = new String[ buf.getInt() ];
			for ( int i = 0; i < array.length; i++ )
			{
				byte strBuffer[] = new byte[ buf.getInt() ];
				buf.get( strBuffer );
				array[i] = new String( strBuffer );
			}
			return array;
		}
		if ( type == ArrayType.BOOL.byteValue() )
		{
			boolean[] array = new boolean[ buf.getInt() ];
			int byteItr = 1;
			byte currentValue = buf.get();
			for ( int i = 0; i < array.length; i++ )
			{
				array[i] = ( currentValue & byteItr ) > 0 ? true : false;
				byteItr *= 2;
				if ( byteItr == 256 )
				{
					byteItr = 0;
					currentValue = buf.get();
				}
			}
			return array;
		}
		if ( type == ArrayType.DOUBLE.byteValue() )
		{
			int size = ( bArray.length - 1 ) / 8;
			assert ( bArray.length - 1 ) % 8 == 0;
			double[] array = new double[size];
			for ( int i = 0; i < size; i++ )
			{
				array[i] = buf.getDouble();
			}
			return array;
		}
		if ( type == ArrayType.FLOAT.byteValue() )
		{
			int size = ( bArray.length - 1 ) / 4;
			assert ( bArray.length - 1 ) % 4 == 0;
			float[] array = new float[size];
			for ( int i = 0; i < size; i++ )
			{
				array[i] = buf.getFloat();
			}
			return array;
		}
		if ( type == ArrayType.LONG.byteValue() )
		{
			int size = ( bArray.length - 1 ) / 8;
			assert ( bArray.length - 1 ) % 8 == 0;
			long[] array = new long[size];
			for ( int i = 0; i < size; i++ )
			{
				array[i] = buf.getLong();
			}
			return array;
		}
		if ( type == ArrayType.BYTE.byteValue() )
		{
			int size = ( bArray.length - 1 );
			byte[] array = new byte[size];
			buf.get( array );
			return array;
		}
		if ( type == ArrayType.CHAR.byteValue() )
		{
			int size = ( bArray.length - 1 ) / 2;
			assert ( bArray.length - 1 ) % 2 == 0;
			char[] array = new char[size];
			for ( int i = 0; i < size; i++ )
			{
				array[i] = buf.getChar();
			}
			return array;
		}
		throw new RuntimeException( "Unkown array type[" + type + "]" );
	}
}

