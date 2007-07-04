package org.neo4j.impl.nioneo.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.neo4j.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource;


public class AdminStore
{
	private static void usage()
	{
		System.out.println( "Nothing to do" );
		System.out.println( "Usage: AdminStore [options...]" );
		System.out.println( "-h, --help\t\t\t"  +
			"Displays this information." );
		System.out.println( "-c, --create [filename]\t\t"  +
			"Creates a new store." );
		System.out.println( "--fsck [filename]\t\t" + 
			"Check and repair a store" );
	}

	public static void main( String args[] ) throws IOException, 
		InstantiationException
	{
		if ( args.length == 0  )
		{
			usage();
			System.exit( 1 );
		}
		performRequest( args );
	}
	
	private static void performRequest( String[] args ) throws 
		IOException, InstantiationException	
	{
		for ( int i = 0; i < args.length; i++ )
		{
			if ( args[i].equals( "-c" ) || args[i].equals( "--create" ) )
			{
				createStore( args[++i] );
			}
			else if ( args[i].equals( "--open" ) )
			{
				Properties properties = new Properties();
				FileInputStream inputStream = new FileInputStream( args[++i] );
				try
				{
					properties.load( inputStream );
					new NeoStoreXaDataSource( properties ).close();
				}
				finally
				{
					inputStream.close();
				}
			}
			else if ( args[i].equals( "--dump-rel-types" ) )
			{
				dumpRelTypes( args[++i] );
			}
			else if ( args[i].equals( "--fsck" ) )
			{
				fsckStore( args[++i] );
			}
			else
			{
				usage();
				return;
			}
		}
	}
	
	private static class DynamicStringStore extends AbstractDynamicStore
	{
		private static final String VERSION = "StringPropertyStore v0.9";
		
		public DynamicStringStore( String fileName, Map config ) 
			throws IOException
		{
			super( fileName, config );
		}

		public DynamicStringStore( String fileName ) 
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
		
		public String getString( int blockId ) throws IOException
		{
			return new String( get( blockId ) );
		}
		
		void rebuildIdGenerators() throws IOException
		{
			rebuildIdGenerator();
		}
	}
	
	private static void dumpRelTypes( String fileName ) throws IOException
	{
		String storeName = fileName + ".relationshiptypestore.db";
		File relTypeStore = new File( storeName );
		if ( !relTypeStore.exists() )
		{
			throw new IOException( "Couldn't find relationship type store " + 
				storeName );
		}
		DynamicStringStore typeNameStore = new DynamicStringStore( 
			storeName + ".names" );
		typeNameStore.rebuildIdGenerators();
		// in_use(byte)+type_blockId(int)
		System.out.println( storeName );
		ByteBuffer buffer = ByteBuffer.allocate( 5 );
		FileChannel fileChannel = 
			new RandomAccessFile( storeName, "rw" ).getChannel();
		fileChannel.position( 0 );
		int i = 0;
		while ( fileChannel.read( buffer ) == 5 )
		{
			buffer.flip();
			byte inUse = buffer.get();
			int block = buffer.getInt();
			String name = "N/A";
			try
			{
				name = typeNameStore.getString( block );
			}
			catch ( IOException e )
			{}
			System.out.println( "ID[" + i + "] use[" + inUse + 
				"] blockId[" + block + "] name[" + name + "]" ); 
			i++;
			buffer.clear();
		}
		typeNameStore.close();
	}
	
	public static void createStore( String fileName ) throws IOException
	{
		NeoStore.createStore( fileName );
	}

	public static void fsckStore( String fileName ) throws IOException
	{
		File neoStore = new File( fileName );
		if ( !neoStore.exists() )
		{
			throw new IOException( "No such neostore " + fileName );
		}
		Set relTypeSet = checkRelTypeStore( 
			fileName + ".relationshiptypestore.db" );
		Set propertySet = checkPropertyStore( fileName + ".propertystore.db" );
		Set nodeSet = checkNodeStore( fileName + ".nodestore.db", propertySet );
		checkRelationshipStore( fileName + ".relationshipstore.db", 
			propertySet, relTypeSet, nodeSet );
		if ( !propertySet.isEmpty() )
		{
			// System.out.print( "Deleting stray properties(" + 
			// 	propertySet.size() + ")" );
			// deleteStrayProperties( fileName, propertySet );
			System.out.println( "Stray properties found : " + 
				propertySet.size() );
		}
		if ( !nodeSet.isEmpty() )
		{
			throw new IOException( "Node(s) with set relationship(s) found " + 
				"but relationships not found in store " + nodeSet.size() );
		}
	}
	
	private static final byte RECORD_NOT_IN_USE = 0;
	private static final byte RECORD_IN_USE = 1;
	private static final int RESERVED = -1;
	
	private static Set checkRelTypeStore( String storeName ) throws IOException
	{
		File relTypeStore = new File( storeName );
		if ( !relTypeStore.exists() )
		{
			throw new IOException( "Couldn't find relationship type store " + 
				storeName );
		}
		File idGenerator = new File( storeName + ".id" );
		if ( idGenerator.exists() )
		{
			idGenerator.delete();
		}
		Set startBlocks = checkDynamicStore( storeName + ".names" );
		// in_use(byte)+type_blockId(int)
		System.out.print( storeName );
		ByteBuffer buffer = ByteBuffer.allocate( 5 );
		FileChannel fileChannel = 
			new RandomAccessFile( storeName, "rw" ).getChannel();
		long fileSize = fileChannel.size();
		fileChannel.position( 0 );
		long dot = fileSize / 5 / 20;
		int i = 0;
		Set<Integer> relTypeSet = new java.util.HashSet<Integer>();
		while ( fileChannel.read( buffer ) == 5 )
		{
			buffer.flip();
			byte inUse = buffer.get();
			if ( inUse == RECORD_IN_USE )
			{
				int block = buffer.getInt();
				if ( block != RESERVED && 
					!startBlocks.remove( block ) )
				{
					throw new IOException( "start block[" + block + 
						"] not found for record " + i );
				}
				relTypeSet.add( i );
			}
			else if ( inUse != RECORD_NOT_IN_USE )
			{
				fileChannel.truncate( fileChannel.position() );
				break;
			}
			i++;
			if ( dot != 0 && i % dot == 0 )
			{
				System.out.print( "." );
			}
			buffer.clear();
		}
		if ( !startBlocks.isEmpty() )
		{
			// throw new IOException( "Stray type name blocks found " +
			//	startBlocks.size() );
			System.out.println( "Stray type name blocks found " +
				startBlocks.size() );
		}
		fileChannel.truncate( i * 5 );
		fileChannel.close();
		System.out.println( ".ok" );
		return relTypeSet;
	}

	private static Set checkPropertyStore( String storeName ) 
		throws IOException
	{
		File relTypeStore = new File( storeName );
		if ( !relTypeStore.exists() )
		{
			throw new IOException( "Couldn't find property store " + 
				storeName );
		}
		File idGenerator = new File( storeName + ".id" );
		if ( idGenerator.exists() )
		{
			idGenerator.delete();
		}
		Set stringStartBlocks = checkDynamicStore( storeName + ".strings" );
		Set keyStartBlocks = checkDynamicStore( storeName + ".keys" );
		// in_use(byte)+type(int)+key_blockId(int)+prop_blockId(long)+
		// prev_prop_id(int)+next_prop_id(int)
		int recordSize = 25;
		System.out.print( storeName );
		ByteBuffer buffer = ByteBuffer.allocate( recordSize );
		FileChannel fileChannel = 
			new RandomAccessFile( storeName, "rw" ).getChannel();
		long fileSize = fileChannel.size();
		fileChannel.position( 0 );
		long dot = fileSize / recordSize / 20;
		Set<Integer> startBlocks = new java.util.HashSet<Integer>();
		int i = 0;
		for ( i = 0; ( i + 1 ) * recordSize <= fileSize; i++ )
		{
			buffer.clear();
			fileChannel.position( i * recordSize );
			fileChannel.read( buffer );
			buffer.flip();
			byte inUse = buffer.get();
			if ( inUse == RECORD_IN_USE )
			{
				int type = buffer.getInt();
				int key = buffer.getInt();
				long prop = buffer.getLong();
				int previous = buffer.getInt();
				int next = buffer.getInt();
				if ( next != NO_NEXT_BLOCK )
				{
					if ( ( next + 1 ) * recordSize > fileSize || 
						next < 0 )
					{
						throw new IOException( "Bad next record[" + next + 
							"] at record " + i );
					}
					buffer.clear();
					fileChannel.position( next * recordSize );
					fileChannel.read( buffer );
					buffer.flip();
					if ( buffer.get() != RECORD_IN_USE )
					{
						throw new IOException( "Bad next record[" + next + 
							",(not in use)] at record " + i );
					}
					buffer.getInt();buffer.getInt();buffer.getLong();
					int prev = buffer.getInt();
					if ( prev != i )
					{
						throw new IOException( "Bad next record[" + next + 
							",(previous don't match)] at record " + i );
					}
				}
				if ( previous == NO_PREV_BLOCK )
				{
					startBlocks.add( i );
				}
				else 
				{
					if ( ( previous + 1 ) * recordSize > fileSize || 
						previous < 0 )
					{
						throw new IOException( "Bad previous record[" + 
							previous + "] at record " + i );
					}
					buffer.clear();
					fileChannel.position( previous * recordSize );
					fileChannel.read( buffer );
					buffer.flip();
					if ( buffer.get() != RECORD_IN_USE )
					{
						throw new IOException( "Bad previous record[" + 
							previous + ",(not in use)] at record " + i );
					}
					buffer.getInt();buffer.getInt();buffer.getLong();
					buffer.getInt();
					int nxt = buffer.getInt();
					if ( nxt != i )
					{
						throw new IOException( "Bad previous record[" + 
							previous + ",(next don't match)] at record " + i );
					}
				}
				if ( type < 1 || type > 6 )
				{
					throw new IOException( "Bad property type[" + type + 
						"] at record " + i );
				}
				if ( !keyStartBlocks.remove( key ) )
				{
					throw new IOException( "key start block[" + key + 
						"] not found for record " + i );
				}
				if ( type == 2 && !stringStartBlocks.remove( (int) prop ) )
				{
					throw new IOException( "string start block[" + prop + 
						"] not found for record " + i );
				}
				
			}
			else if ( inUse != RECORD_NOT_IN_USE )
			{
				throw new IOException( "Bad record at " + i );
			}
			if ( dot != 0 && i % dot == 0 )
			{
				System.out.print( "." );
			}
		}
		if ( !stringStartBlocks.isEmpty() )
		{
			System.out.println( "Stray string blocks found " +
				stringStartBlocks.size() );
			// throw new IOException( "Stray string blocks found " +
			//	stringStartBlocks.size() );
		}
		if ( !keyStartBlocks.isEmpty() )
		{
			System.out.println( "Stray key blocks found " +
				keyStartBlocks.size() );
			// throw new IOException( "Stray key blocks found " +
			//	keyStartBlocks.size() );
		}
		fileChannel.truncate( i * recordSize );
		fileChannel.close();
		System.out.println( ".ok" );
		return startBlocks;
	}

	private static Set checkNodeStore( String storeName, Set propertySet ) 
		throws IOException
	{
		File relTypeStore = new File( storeName );
		if ( !relTypeStore.exists() )
		{
			throw new IOException( "Couldn't find node store " + 
				storeName );
		}
		File idGenerator = new File( storeName + ".id" );
		if ( idGenerator.exists() )
		{
			idGenerator.delete();
		}
		// in_use(byte)+next_rel_id(int)+next_prop_id(int)
		int recordSize = 9;
		System.out.print( storeName );
		ByteBuffer buffer = ByteBuffer.allocate( recordSize );
		FileChannel fileChannel = 
			new RandomAccessFile( storeName, "rw" ).getChannel();
		long fileSize = fileChannel.size();
		fileChannel.position( 0 );
		long dot = fileSize / recordSize / 20;
		Set<Integer> nodeSet = new java.util.HashSet<Integer>();
		int i = 0;
		for ( i = 0; ( i + 1 ) * recordSize <= fileSize; i++ )
		{
			buffer.clear();
			fileChannel.position( i * recordSize );
			fileChannel.read( buffer );
			buffer.flip();
			byte inUse = buffer.get();
			if ( inUse == RECORD_IN_USE )
			{
				int nextRel = buffer.getInt();
				int nextProp = buffer.getInt();
				if ( nextRel != NO_NEXT_RELATIONSHIP )
				{
					nodeSet.add( i );
				}
				if ( nextProp != NO_NEXT_PROPERTY && 
					!propertySet.remove( nextProp ) )
				{
					throw new IOException( "Bad property start block[" + 
						nextProp + "] on record " + i );
				}
			}
			else if ( inUse != RECORD_NOT_IN_USE )
			{
				buffer.clear();
				buffer.put( RECORD_NOT_IN_USE );
				buffer.putInt( NO_NEXT_RELATIONSHIP );
				buffer.putInt( NO_NEXT_PROPERTY );
				buffer.flip();
				fileChannel.position( i * recordSize );
				fileChannel.write( buffer );
				System.out.print( "o" );
			}
			if ( dot != 0 && i % dot == 0 )
			{
				System.out.print( "." );
			}
		}
		fileChannel.truncate( i * recordSize );
		fileChannel.close();
		System.out.println( ".ok" );
		return nodeSet;
	}
	
	private static final byte NOT_DIRECTED = 0;
	private static final byte DIRECTED = 2;
	private static final int  NO_NEXT_RELATIONSHIP = -1;
	private static final int  NO_PREVIOUS_RELATIONSHIP = -1;
	private static final int  NO_NEXT_PROPERTY = -1;

	private static void checkRelationshipStore( String storeName, 
		Set propertySet, Set relTypeSet, Set nodeSet ) throws IOException
	{
		File relStore = new File( storeName );
		if ( !relStore.exists() )
		{
			throw new IOException( "Couldn't find relationship store " + 
				storeName );
		}
		File idGenerator = new File( storeName + ".id" );
		if ( idGenerator.exists() )
		{
			idGenerator.delete();
		}
	// directed|in_use(byte)+first_node(int)+second_node(int)+rel_type(int)+
	// first_prev_rel(int)+first_next_rel(int) 
	// second_prev_rel(int)+second_next_rel(int)+next_prop_id(int)
		int recordSize = 33;
		System.out.print( storeName );
		ByteBuffer buffer = ByteBuffer.allocate( recordSize );
		FileChannel fileChannel = 
			new RandomAccessFile( relStore, "rw" ).getChannel();
		long fileSize = fileChannel.size();
		fileChannel.position( 0 );
		long dot = fileSize / recordSize / 20;
		int i = 0;
		for ( i = 0; ( i + 1 ) * recordSize <= fileSize; i++ )
		{
			buffer.clear();
			fileChannel.position( i * recordSize );
			fileChannel.read( buffer );
			buffer.flip();
			byte inUse = buffer.get();
			if ( inUse == RECORD_IN_USE + NOT_DIRECTED || inUse == 
				RECORD_IN_USE + DIRECTED )
			{
				int firstNode = buffer.getInt();
				int secondNode = buffer.getInt();
				int type = buffer.getInt();
				int firstPrev = buffer.getInt();
				int firstNext = buffer.getInt();
				int secondPrev = buffer.getInt();
				int secondNext = buffer.getInt();
				int prop = buffer.getInt();
				if ( firstPrev == NO_PREVIOUS_RELATIONSHIP && 
					!nodeSet.remove( firstNode ) )
				{
					throw new IOException( "Bad start node[" + firstNode + 
						"](node don't exist or don't have relationships) " +
						"at record " + i );
				}
				if ( secondPrev == NO_PREVIOUS_RELATIONSHIP && 
					!nodeSet.remove( secondNode ) )
				{
					throw new IOException( "Bad start node[" + secondNode + 
						"](node don't exist or don't have relationships) " +
						"at record " + i );
				}
				checkRelationshipList( i, firstNode, firstPrev, firstNext, 
					fileChannel, buffer );
				checkRelationshipList( i, secondNode, secondPrev, secondNext, 
					fileChannel, buffer );
				if ( prop != NO_NEXT_PROPERTY && 
					!propertySet.remove( prop ) )
				{
					throw new IOException( "Bad property start block[" + 
						prop + "] on record " + i );
				}
				if ( !relTypeSet.contains( type ) )
				{
					throw new IOException( "Bad rel type[" + type + 
						"] on record " + i );
				}
			}
			else if ( inUse != RECORD_NOT_IN_USE )
			{
				throw new IOException( "Bad record at " + i );
			}
			if ( dot != 0 && i % dot == 0 )
			{
				System.out.print( "." );
			}
		}
		fileChannel.truncate( i * recordSize );
		fileChannel.close();
		System.out.println( ".ok" );
	}
	
	private static void checkRelationshipList( int i, int node, int prev, 
		int next, FileChannel fileChannel, ByteBuffer buffer ) 
		throws IOException
	{
		long fileSize = fileChannel.size();
		int recordSize = 33;
		if ( next != NO_NEXT_BLOCK )
		{
			if ( ( next + 1 ) * recordSize > fileSize || 
				next < 0 )
			{
				throw new IOException( "Bad next record[" + next + 
					"] at record " + i );
			}
			buffer.clear();
			fileChannel.position( next * recordSize );
			fileChannel.read( buffer );
			buffer.flip();
			byte inUse = buffer.get();
			if ( inUse != RECORD_IN_USE + NOT_DIRECTED && inUse != 
				RECORD_IN_USE + DIRECTED )
			{
				throw new IOException( "Bad next record[" + next + 
					",(not in use)] at record " + i );
			}
			int firstNode = buffer.getInt();
			int secondNode = buffer.getInt();
			buffer.getInt();
			int firstPrev = buffer.getInt();
			buffer.getInt(); // firstNext
			int secondPrev = buffer.getInt();
			buffer.getInt(); // secondNext
			if ( firstNode != node && secondNode != node )
			{
				throw new IOException( "Bad next record[" + next + 
					"],(nodes don't match)] at record " + i );
			}
			if ( firstNode == node && firstPrev != i )
			{
				throw new IOException( "Bad next record[" + next + 
					",(previous don't match)] at record " + i );
			}
			if ( secondNode == node && secondPrev != i )
			{
				throw new IOException( "Bad next record[" + next + 
					",(previous don't match)] at record " + i );
			}
		}
		if ( prev != NO_PREV_BLOCK )
		{
			if ( ( prev + 1 ) * recordSize > fileSize || prev < 0 )
			{
				throw new IOException( "Bad previous record[" + 
					prev + "] at record " + i );
			}
			buffer.clear();
			fileChannel.position( prev * recordSize );
			fileChannel.read( buffer );
			buffer.flip();
			byte inUse = buffer.get();
			if ( inUse != RECORD_IN_USE + NOT_DIRECTED && inUse != 
				RECORD_IN_USE + DIRECTED )
			{
				throw new IOException( "Bad previous record[" + 
					prev + ",(not in use)] at record " + i );
			}
			int firstNode = buffer.getInt();
			int secondNode = buffer.getInt();
			buffer.getInt();
			buffer.getInt(); // firstPrev
			int firstNext = buffer.getInt();
			buffer.getInt(); // secondPrev
			int secondNext = buffer.getInt();
			if ( firstNode != node && secondNode != node )
			{
				throw new IOException( "Bad next record[" + next + 
					"],(nodes don't match)] at record " + i );
			}
			if ( firstNode == node && firstNext != i )
			{
				throw new IOException( "Bad previous record[" + prev + 
					",(next don't match)] at record " + i );
			}
			if ( secondNode == node && secondNext != i )
			{
				throw new IOException( "Bad previous record[" + prev + 
					",(next don't match)] at record " + i );
			}
		}
	}

	// in_use(byte)+prev_block(int)+nr_of_bytes(int)+next_block(int) 
	private static final int BLOCK_HEADER_SIZE = 1 + 4 + 4 + 4;
	private static final int NO_NEXT_BLOCK = -1;
	private static final int NO_PREV_BLOCK = -1;
	private static final byte BLOCK_IN_USE = (byte) 1;
	private static final byte BLOCK_NOT_IN_USE = (byte) 0;

	private static Set checkDynamicStore( String storeName ) throws IOException
	{
		File dynamicStore = new File( storeName );
		if ( !dynamicStore.exists() )
		{
			throw new IOException( "Couldn't find dynamic store " + 
				storeName );
		}
		File idGenerator = new File( storeName + ".blockid" );
		if ( idGenerator.exists() )
		{
			idGenerator.delete();
		}
		System.out.print( storeName );
		FileChannel fileChannel = 
			new RandomAccessFile( storeName, "rw" ).getChannel();
		ByteBuffer buffer = ByteBuffer.allocate( 4 );
		fileChannel.position( 0 );
		if ( fileChannel.read( buffer ) != 4 )
		{
			throw new IOException( "Unable to read blocksize" );
		}
		buffer.flip();
		int blockSize = buffer.getInt();
		int dataSize = blockSize - BLOCK_HEADER_SIZE;
		long fileSize = fileChannel.size();
		buffer = ByteBuffer.allocate( BLOCK_HEADER_SIZE );
		ByteBuffer inUseBuffer = ByteBuffer.allocate( 1 );
		long dot = fileSize / blockSize / 20;
		Set<Integer> startBlocks = new java.util.HashSet<Integer>();
		int i = 0;
		for ( i = 1; ( i + 1 ) * blockSize <= fileSize; i++ )
		{
			inUseBuffer.clear();
			fileChannel.position( i * blockSize );
			fileChannel.read( inUseBuffer );
			inUseBuffer.flip();
			byte inUse = inUseBuffer.get();
			if ( inUse == BLOCK_IN_USE )
			{
				buffer.clear();
				fileChannel.read( buffer );
				buffer.flip();
				int previous = buffer.getInt();
				int bytes = buffer.getInt();
				int next = buffer.getInt();
				if ( next != NO_NEXT_BLOCK && bytes != dataSize || 
					bytes > dataSize || bytes < 0 )
				{
					throw new IOException( "Bad data size[" + bytes + 
						"] at block " + i + " in " + storeName );
				}
				else if ( next != NO_NEXT_BLOCK )
				{
					if ( ( next + 1 ) * blockSize > fileSize || 
						next < 0 )
					{
						throw new IOException( "Bad next block[" + next + 
							"] at block " + i );
					}
					buffer.clear();
					fileChannel.position( next * blockSize );
					fileChannel.read( buffer );
					buffer.flip();
					if ( buffer.get() != BLOCK_IN_USE )
					{
						throw new IOException( "Bad next block[" + next + 
							",(not in use)] at block " + i );
					}
					int prev = buffer.getInt();
					if ( prev != i )
					{
						throw new IOException( "Bad next block[" + next + 
							",(previous don't match)] at block " + i );
					}
				}
				if ( previous == NO_PREV_BLOCK )
				{
					startBlocks.add( i );
				}
				else 
				{
					if ( ( previous + 1 ) * blockSize > fileSize || 
						previous < 0 )
					{
						throw new IOException( "Bad previous block[" + 
							previous + "] at block " + i );
					}
					buffer.clear();
					fileChannel.position( previous * blockSize );
					fileChannel.read( buffer );
					buffer.flip();
					if ( buffer.get() != BLOCK_IN_USE )
					{
						throw new IOException( "Bad previous block[" + 
							previous + ",(not in use)] at block " + i );
					}
					buffer.getInt();
					buffer.getInt();
					int nxt = buffer.getInt();
					if ( nxt != i )
					{
						throw new IOException( "Bad previous block[" + 
							previous + ",(next don't match)] at block " + i );
					}
				}
			}
			else if ( inUse != BLOCK_NOT_IN_USE )
			{
				throw new IOException( "Bad block at " + i );
			}
			if ( dot != 0 && i % dot == 0 )
			{
				System.out.print( "." );
			}
		}
		fileChannel.truncate( i * blockSize );
		fileChannel.close();
		System.out.println( ".ok" );
		return startBlocks;
	}
	
/*	private static void deleteStrayProperties( String storeName, 
		Set propertySet ) throws IOException
	{
		FileChannel propChannel = new RandomAccessFile( 
			storeName + ".propertystore.db", "rw" ).getChannel();
		ByteBuffer propBuffer = ByteBuffer.allocate( 25 );
		
		FileChannel keyChannel = new RandomAccessFile( 
			storeName + ".propertystore.db.keys", "rw" ).getChannel();
		ByteBuffer keyBuffer = ByteBuffer.allocate( BLOCK_HEADER_SIZE );
		keyBuffer.limit( 4 );
		keyChannel.position( 0 );
		if ( keyChannel.read( keyBuffer ) != 4 )
		{
			throw new IOException( "Unable to read blocksize" );
		}
		keyBuffer.flip();
		int keyBlockSize = keyBuffer.getInt();
		
		FileChannel stringChannel = new RandomAccessFile( 
			storeName + ".propertystore.db.strings", "rw" ).getChannel();
		ByteBuffer stringBuffer = ByteBuffer.allocate( BLOCK_HEADER_SIZE );
		stringBuffer.limit( 4 );
		stringChannel.position( 0 );
		if ( stringChannel.read( stringBuffer ) != 4 )
		{
			throw new IOException( "Unable to read blocksize" );
		}
		stringBuffer.flip();
		int stringBlockSize = stringBuffer.getInt();

		int dot = propertySet.size() / 20;
		int i = 0;
		java.util.Iterator itr = propertySet.iterator();
		while ( itr.hasNext() )
		{
			deletePropertyChain( ( (Integer) itr.next()).intValue(), 
				propChannel, propBuffer, keyChannel, keyBuffer, keyBlockSize, 
				stringChannel, stringBuffer, stringBlockSize );
			if ( dot != 0 && i % dot == 0 )
			{
				System.out.print( "." );
			}
			i++;
		}
		System.out.println( ".ok" );
	}*/
	
/*	private static void deletePropertyChain( int startId, 
		FileChannel propChannel, ByteBuffer propBuffer, 
		FileChannel keyChannel, ByteBuffer keyBuffer, int keyBlockSize, 
		FileChannel stringChannel, ByteBuffer stringBuffer, int stringBlockSize ) 
		throws IOException
	{
		int id = startId;
		int recordSize = propBuffer.capacity();
		do
		{
			propBuffer.clear();
			propChannel.position( id * recordSize );
			propChannel.read( propBuffer );
			propBuffer.flip();
			byte inUse = propBuffer.get();
			if ( inUse != RECORD_IN_USE )
			{
				throw new IOException( "record " + id + " not in use" );
			}
			int type = propBuffer.getInt();
			int key = propBuffer.getInt();
			long prop = propBuffer.getLong();
			propBuffer.getInt();
			int next = propBuffer.getInt();
			propBuffer.flip();
			propBuffer.put( RECORD_NOT_IN_USE );
			propBuffer.flip();
			propChannel.position( id * recordSize );
			propChannel.write( propBuffer );
			id = next;
			clearDynamicBlocks( key, keyChannel, keyBuffer, keyBlockSize );
			if ( type == 2 )
			{
				clearDynamicBlocks( (int) prop, stringChannel, stringBuffer, 
					stringBlockSize );
			}
		}
		while ( id != NO_NEXT_PROPERTY );
	}*/
	
/*	private static void clearDynamicBlocks( int id, FileChannel fileChannel, 
		ByteBuffer buffer, int blockSize ) throws IOException
	{
		do
		{
			buffer.clear();
			fileChannel.position( id * blockSize );
			fileChannel.read( buffer );
			buffer.flip();
			byte inUse = buffer.get();
			if ( inUse != BLOCK_IN_USE )
			{
				throw new IOException( "block " + id + " not in use" );
			}
			buffer.getInt();
			buffer.getInt();
			int next = buffer.getInt();
			buffer.flip();
			buffer.put( RECORD_NOT_IN_USE );
			buffer.flip();
			fileChannel.position( id * blockSize );
			fileChannel.write( buffer );
			id = next;
		}
		while ( id != NO_NEXT_BLOCK );
	}*/
}
