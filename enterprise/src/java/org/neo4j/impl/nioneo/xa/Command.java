package org.neo4j.impl.nioneo.xa;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.logging.Logger;
import org.neo4j.impl.nioneo.store.DynamicRecord;
import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.store.NodeRecord;
import org.neo4j.impl.nioneo.store.NodeStore;
import org.neo4j.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.impl.nioneo.store.PropertyRecord;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.PropertyType;
import org.neo4j.impl.nioneo.store.Record;
import org.neo4j.impl.nioneo.store.RelationshipRecord;
import org.neo4j.impl.nioneo.store.RelationshipStore;
import org.neo4j.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.impl.transaction.xaframework.LogBuffer;
import org.neo4j.impl.transaction.xaframework.XaCommand;

/**
 * Command implementations for all the commands that can be performed on a 
 * Neo store.
 */
abstract class Command extends XaCommand
{
	static Logger logger = Logger.getLogger( Command.class.getName() );
	
	private final int key;
	private boolean isInRecovery = false;
	
	Command( int key )
	{
		this.key = key;
	}
	
	boolean isInRecoveryMode()
	{
		return isInRecovery;
	}
	
	void setIsInRecoveryMode()
	{
		isInRecovery = true;
	}
	
	int getKey()
	{
		return key;
	}
	
//	private volatile int hashCode = 0;

	public int hashCode()
	{
		return key;
//		if ( hashCode == 0 )
//		{
//			hashCode = 3217 * key;
//		}
//		return hashCode;
	}
	
	
//	static void writeDynamicRecord( FileChannel fileChannel, 
//		DynamicRecord record, ByteBuffer buffer ) throws IOException 
	static void writeDynamicRecord( LogBuffer buffer, DynamicRecord record ) 
		throws IOException 
	{
		// id+in_use(byte)+prev_block(int)+nr_of_bytes(int)+next_block(int) 
//		buffer.clear();
//		record.setTransferStartPosition( fileChannel, 
//			fileChannel.position() + 4 );
		record.setTransferStartPosition( buffer.getFileChannel(), 
			buffer.getFileChannelPosition() + 4 );
		if ( record.inUse() )
		{
			// byte inUse = record.inUse() ? 
			// Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
			byte inUse = Record.IN_USE.byteValue();
			buffer.putInt( record.getId() ).put( inUse ).putInt( 
				record.getPrevBlock() ).putInt( record.getLength() ).putInt( 
				record.getNextBlock() );
			if ( !record.isLight() )
			{
				if ( !record.isCharData() )
				{
					byte[] data = record.getData();
					buffer.put( data );
					record.setTransferCount( 13 + data.length );
				}
				else
				{
					char[] chars = record.getDataAsChar();
					buffer.put( chars );
					record.setTransferCount( 13 + chars.length * 2 );
				}
			}
			else
			{
				record.setTransferCount( 13 );
			}
		}
		else
		{
			byte inUse = Record.NOT_IN_USE.byteValue();
			buffer.putInt( record.getId() ).put( inUse );
			record.setTransferCount( 1 );
		}
//		buffer.flip();
//		fileChannel.write( buffer );
	}
	
	static DynamicRecord readDynamicRecord( FileChannel fileChannel, 
		ByteBuffer buffer) throws IOException
	{
		// id+in_use(byte)+prev_block(int)+nr_of_bytes(int)+next_block(int)
		buffer.clear(); buffer.limit( 5 );
		if ( fileChannel.read( buffer ) != buffer.limit() )
		{
			return null;
		}
		buffer.flip();
		int id = buffer.getInt();
		byte inUseFlag = buffer.get();
		boolean inUse = false;
		if ( inUseFlag == Record.IN_USE.byteValue() )
		{
			inUse = true;
			buffer.clear();
			buffer.limit( 12 );
			if ( fileChannel.read( buffer ) != buffer.limit() )
			{
				return null;
			}
			buffer.flip();
		}
		else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
		{
			throw new IOException( "Illegal in use flag: " + inUseFlag );
		}
		DynamicRecord record = new DynamicRecord( id );
		record.setInUse( inUse );
		if ( inUse )
		{
			record.setPrevBlock( buffer.getInt() );
			int nrOfBytes = buffer.getInt();
			record.setNextBlock( buffer.getInt() );
			buffer.clear(); buffer.limit( nrOfBytes );
			if ( fileChannel.read( buffer ) != buffer.limit() )
			{
				return null;
			}
			buffer.flip();
			byte data[] = new byte[ nrOfBytes ];
			buffer.get( data );
			record.setData( data );
		}
		return record;
	}

	private static final byte NODE_COMMAND = (byte) 1;
	private static final byte PROP_COMMAND = (byte) 2;
	private static final byte REL_COMMAND = (byte) 3;
	private static final byte REL_TYPE_COMMAND = (byte) 4;
	private static final byte PROP_INDEX_COMMAND = (byte) 5;
	
	static class NodeCommand extends Command
	{
		private final NodeRecord record;
		private final NodeStore store;
		
		NodeCommand( NodeStore store, NodeRecord record )
		{
			super( record.getId() );
			this.record = record;
			this.store = store;
		}

		@Override
		public void execute()
		{
			if ( isInRecoveryMode() )
			{
				logger.fine( this.toString() );
			}
			try
			{
				store.updateRecord( record );
			}
			catch ( IOException e )
			{
				throw new RuntimeException( e );
			}
		}
		
		public String toString()
		{
			return "NodeCommand[" + record + "]";
		}
		

		@Override
//		public void writeToFile( FileChannel fileChannel, ByteBuffer buffer ) 
//			throws IOException
		public void writeToFile( LogBuffer buffer ) throws IOException
		{
//			buffer.clear();
			byte inUse = record.inUse() ? 
					Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
			buffer.put( NODE_COMMAND );
			buffer.putInt( record.getId() ).put( inUse );
//			record.setTransferStartPosition( fileChannel, 
//				fileChannel.position() + 5 );
			record.setTransferStartPosition( buffer.getFileChannel(), 
				buffer.getFileChannelPosition() + 5 );
			if ( record.inUse() )
			{
				buffer.putInt( record.getNextRel() ).putInt( 
					record.getNextProp() );				
				record.setTransferCount( 9 );
			}
			else
			{
				record.setTransferCount( 1 );
			}
//			buffer.flip();
//			fileChannel.write( buffer );
		}
		
		static Command readCommand( NeoStore neoStore, FileChannel fileChannel, 
			ByteBuffer buffer ) throws IOException
		{
			buffer.clear(); buffer.limit( 13 );
			if ( fileChannel.read( buffer ) != buffer.limit() )
			{
				return null;
			}
			buffer.flip();
			int id = buffer.getInt();
			byte inUseFlag = buffer.get();
			boolean inUse = false;
			if ( inUseFlag == Record.IN_USE.byteValue() )
			{
				inUse = true;
			}
			else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
			{
				throw new IOException( "Illegal in use flag: " + inUseFlag );
			}
			NodeRecord record = new NodeRecord( id );
			record.setInUse( inUse );
			if ( inUse )
			{
				record.setNextRel( buffer.getInt() );
				record.setNextProp( buffer.getInt() );
			}
			return new NodeCommand( neoStore.getNodeStore(), record );
		}
		
		public boolean equals( Object o )
		{
			if ( !( o instanceof NodeCommand ) )
			{
				return false;
			}
			//return getKey().equals( ( ( NodeCommand ) o ).getKey() );
			return getKey() == ((Command) o).getKey();
		}
	
	}
	
	static class RelationshipCommand extends Command
	{
		private final RelationshipRecord record;
		private final RelationshipStore store;
		
		RelationshipCommand( RelationshipStore store, 
			RelationshipRecord record )
		{
			super( record.getId() );
			this.record = record;
			this.store = store;
		}

		@Override
		public void execute()
		{
			if ( isInRecoveryMode() )
			{
				logger.fine( this.toString() );
			}
			try
			{
				store.updateRecord( record );
			}
			catch ( IOException e )
			{
				throw new RuntimeException( e );
			}
		}

		@Override
		public String toString()
		{
			return "RelationshipCommand[" + record + "]";
		}

		@Override
//		public void writeToFile( FileChannel fileChannel, ByteBuffer buffer ) 
//			throws IOException
		public void writeToFile( LogBuffer buffer ) throws IOException
		{
//			buffer.clear();
			byte inUse = record.inUse() ? 
				Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
			buffer.put( REL_COMMAND );
			
			buffer.putInt( record.getId() ).put( inUse );
//			record.setTransferStartPosition( fileChannel, 
//				fileChannel.position() + 5 );
			record.setTransferStartPosition( buffer.getFileChannel(), 
				buffer.getFileChannelPosition() + 5 );
			if ( record.inUse() )
			{
				buffer.putInt( record.getFirstNode() ).putInt( 
					record.getSecondNode() ).putInt( record.getType() ).putInt( 
					record.getFirstPrevRel() ).putInt( 
					record.getFirstNextRel() ).putInt( 
					record.getSecondPrevRel() ).putInt( 
					record.getSecondNextRel() ).putInt( record.getNextProp() );				
				record.setTransferCount( 33 );
			}
			else
			{
				record.setTransferCount( 1 );
			}
//			buffer.flip();
//			fileChannel.write( buffer );
		}
		
		static Command readCommand( NeoStore neoStore, FileChannel fileChannel, 
			ByteBuffer buffer ) throws IOException
		{
			buffer.clear(); buffer.limit( 37 );
			if ( fileChannel.read( buffer ) != buffer.limit() )
			{
				return null;
			}
			buffer.flip();
			int id = buffer.getInt();
			byte inUseFlag = buffer.get();
			boolean inUse = false;
			if ( ( inUseFlag & Record.IN_USE.byteValue() ) == 
				Record.IN_USE.byteValue() )
			{
				inUse = true;
			}
			else if ( ( inUseFlag & Record.IN_USE.byteValue() ) != 
				Record.NOT_IN_USE.byteValue() )
			{
				throw new IOException( "Illegal in use flag: " + inUseFlag );
			}
			RelationshipRecord record;
			if ( inUse )
			{
				record = new RelationshipRecord( id, buffer.getInt(), 
					buffer.getInt(), buffer.getInt() );
				record.setInUse( inUse );
				record.setFirstPrevRel( buffer.getInt() );
				record.setFirstNextRel( buffer.getInt() );
				record.setSecondPrevRel( buffer.getInt() );
				record.setSecondNextRel( buffer.getInt() );
				record.setNextProp( buffer.getInt() );
			}
			else
			{
				record = new RelationshipRecord( id, -1, -1, -1 );
				record.setInUse( false );
			}
			return new RelationshipCommand( neoStore.getRelationshipStore(), 
					record );
		}

		@Override
		public boolean equals( Object o )
		{
			if ( !( o instanceof RelationshipCommand ) )
			{
				return false;
			}
//			return getKey().equals( ( ( RelationshipCommand ) o ).getKey() );
			return getKey() == ((Command) o).getKey();
		}	
	}	
	
	static class PropertyIndexCommand extends Command
	{
		private final PropertyIndexRecord record;
		private final PropertyIndexStore store;
		
		PropertyIndexCommand( PropertyIndexStore store, 
			PropertyIndexRecord record )
		{
			super( record.getId() );
			this.record = record;
			this.store = store;
		}

		@Override
		public void execute()
		{
			if ( isInRecoveryMode() )
			{
				logger.fine( this.toString() );
			}
			try
			{
				store.updateRecord( record );
			}
			catch ( IOException e )
			{
				throw new RuntimeException( e );
			}
		}

		@Override
		public String toString()
		{
			return "PropertyCommand[" + record + "]";
		}

		@Override
//		public void writeToFile( FileChannel fileChannel, ByteBuffer buffer ) 
//			throws IOException
		public void writeToFile( LogBuffer buffer ) throws IOException
		{
			// id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
// 			buffer.clear();
			byte inUse = record.inUse() ? 
				Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
			buffer.put( PROP_INDEX_COMMAND );
			buffer.putInt( record.getId() ).put( inUse );
			buffer.putInt( record.getPropertyCount() ).putInt( 
				record.getKeyBlockId() );
			if ( record.isLight() )
			{
				buffer.putInt( 0 );
//				buffer.flip();
//				fileChannel.write( buffer );
			}
			else
			{
				Collection<DynamicRecord> keyRecords = 
					record.getKeyRecords();
				buffer.putInt( keyRecords.size() );
//				buffer.flip();
//				fileChannel.write( buffer );
				for ( DynamicRecord keyRecord : keyRecords )
				{
//					writeDynamicRecord( fileChannel, keyRecord, buffer );
					writeDynamicRecord( buffer, keyRecord );
				}
			}
		}
		
		static Command readCommand( NeoStore neoStore, FileChannel fileChannel, 
			ByteBuffer buffer ) throws IOException
		{
			// id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
			buffer.clear(); buffer.limit( 17 );
			if ( fileChannel.read( buffer ) != buffer.limit() )
			{
				return null;
			}
			buffer.flip();
			int id = buffer.getInt();
			byte inUseFlag = buffer.get();
			boolean inUse = false;
			if ( ( inUseFlag & Record.IN_USE.byteValue() ) == 
				Record.IN_USE.byteValue() )
			{
				inUse = true;
			}
			else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
			{
				throw new IOException( "Illegal in use flag: " + inUseFlag );
			}
			PropertyIndexRecord record = new PropertyIndexRecord( id );
			record.setInUse( inUse );
			record.setPropertyCount( buffer.getInt() );
			record.setKeyBlockId( buffer.getInt() );
			int nrKeyRecords = buffer.getInt();
			for ( int i = 0; i < nrKeyRecords; i++ )
			{
				DynamicRecord dr = readDynamicRecord( fileChannel, buffer );
				if ( dr == null )
				{
					return null;
				}
				record.addKeyRecord( dr );
			}
			return new PropertyIndexCommand( 
				neoStore.getPropertyStore().getIndexStore(), record );
		}

		@Override
		public boolean equals( Object o )
		{
			if ( !( o instanceof PropertyCommand ) )
			{
				return false;
			}
			return getKey() == ((Command) o).getKey();
		}	
	}

	static class PropertyCommand extends Command
	{
		private final PropertyRecord record;
		private final PropertyStore store;
		
		PropertyCommand( PropertyStore store, PropertyRecord record )
		{
			super( record.getId() );
			this.record = record;
			this.store = store;
		}

		@Override
		public void execute()
		{
			if ( isInRecoveryMode() )
			{
				logger.fine( this.toString() );
			}
			try
			{
				store.updateRecord( record );
			}
			catch ( IOException e )
			{
				throw new RuntimeException( e );
			}
		}

		@Override
		public String toString()
		{
			return "PropertyCommand[" + record + "]";
		}
		
		@Override
//		public void writeToFile( FileChannel fileChannel, ByteBuffer buffer ) 
//			throws IOException
		public void writeToFile( LogBuffer buffer ) throws IOException
		{
			// id+in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
			// prev_prop_id(int)+next_prop_id(int)+nr_value_records(int)
//			buffer.clear();
			byte inUse = record.inUse() ? 
				Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
			buffer.put( PROP_COMMAND );
			buffer.putInt( record.getId() ).put( inUse );
//			record.setTransferStartPosition( fileChannel, 
//				fileChannel.position() + 5 );
			record.setTransferStartPosition( buffer.getFileChannel(), 
				buffer.getFileChannelPosition() + 5 );
			if ( record.inUse() )
			{
				buffer.putInt( record.getType().intValue() ).putInt( 
					record.getKeyIndexId() ).putLong( 
					record.getPropBlock() ).putInt( 
					record.getPrevProp() ).putInt( record.getNextProp() );
				record.setTransferCount( 25 );
			}
			else
			{
				record.setTransferCount( 1 );
			}
			if ( record.isLight() )
			{
				buffer.putInt( 0 );
//				buffer.flip();
//				fileChannel.write( buffer );
			}
			else
			{
				Collection<DynamicRecord> valueRecords = 
					record.getValueRecords();
				buffer.putInt( valueRecords.size() );
//				buffer.flip();
//				fileChannel.write( buffer );
				for ( DynamicRecord valueRecord : valueRecords )
				{
//					writeDynamicRecord( fileChannel, valueRecord, buffer );
					writeDynamicRecord( buffer, valueRecord );
				}
			}
		}
		
		static Command readCommand( NeoStore neoStore, FileChannel fileChannel, 
			ByteBuffer buffer ) throws IOException
		{
			// id+in_use(byte)+type(int)+key_indexId(int)+prop_blockId(long)+
			// prev_prop_id(int)+next_prop_id(int)+nr_value_records(int)
			buffer.clear(); buffer.limit( 33 );
			if ( fileChannel.read( buffer ) != buffer.limit() )
			{
				return null;
			}
			buffer.flip();
			int id = buffer.getInt();
			byte inUseFlag = buffer.get();
			boolean inUse = false;
			if ( ( inUseFlag & Record.IN_USE.byteValue() ) == 
				Record.IN_USE.byteValue() )
			{
				inUse = true;
			}
			else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
			{
				throw new IOException( "Illegal in use flag: " + inUseFlag );
			}
			PropertyRecord record = new PropertyRecord( id );
			if ( inUse )
			{
				PropertyType type = getType( buffer.getInt() );
				record.setType( type );
				record.setInUse( inUse );
				record.setKeyIndexId( buffer.getInt() );
				record.setPropBlock( buffer.getLong() );
				record.setPrevProp( buffer.getInt() );
				record.setNextProp( buffer.getInt() );
			}
			int nrValueRecords = buffer.getInt();
			for ( int i = 0; i < nrValueRecords; i++ )
			{
				DynamicRecord dr = readDynamicRecord( fileChannel, buffer );
				if ( dr == null )
				{
					return null;
				}
				record.addValueRecord( dr );
			}
			return new PropertyCommand( neoStore.getPropertyStore(), 
					record );
		}
		
		private static PropertyType getType( int type )
		{
			switch ( type )
			{
			case 1: return PropertyType.INT;
			case 2: return PropertyType.STRING;
			case 3: return PropertyType.BOOL;
			case 4: return PropertyType.DOUBLE;
			case 5: return PropertyType.FLOAT;
			case 6: return PropertyType.LONG;
			case 7: return PropertyType.BYTE;
			case 8: return PropertyType.CHAR;
			case 9: return PropertyType.ARRAY;
			}
			throw new RuntimeException( "Unkown property type:" + type );
		}

		@Override
		public boolean equals( Object o )
		{
			if ( !( o instanceof PropertyCommand ) )
			{
				return false;
			}
			return getKey() == ((Command) o).getKey();
//			return getKey().equals( ( ( PropertyCommand ) o ).getKey() );
		}	
	}	
	
	static class RelationshipTypeCommand extends Command
	{
		private final RelationshipTypeRecord record;
		private final RelationshipTypeStore store;
		
		RelationshipTypeCommand( RelationshipTypeStore store, 
				RelationshipTypeRecord record )
		{
			super( record.getId() );
			this.record = record;
			this.store = store;
		}

		@Override
		public void execute()
		{
			if ( isInRecoveryMode() )
			{
				logger.fine( this.toString() );
			}
			try
			{
				store.updateRecord( record );
			}
			catch ( IOException e )
			{
				throw new RuntimeException( e );
			}
		}

		@Override
		public String toString()
		{
			return "RelationshipTypeCommand[" + record + "]";
		}

		@Override
//		public void writeToFile( FileChannel fileChannel, ByteBuffer buffer ) 
//			throws IOException
		public void writeToFile( LogBuffer buffer ) throws IOException
		{
			// id+in_use(byte)+type_blockId(int)+nr_type_records(int)
//			buffer.clear();
			byte inUse = record.inUse() ? 
				Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
			buffer.put( REL_TYPE_COMMAND );
			buffer.putInt( record.getId() ).put( inUse ).putInt( 
				record.getTypeBlock() );
			
			Collection<DynamicRecord> typeRecords = record.getTypeRecords();
			buffer.putInt( typeRecords.size() );
//			buffer.flip();
//			fileChannel.write( buffer );
			for ( DynamicRecord typeRecord : typeRecords )
			{
//				writeDynamicRecord( fileChannel, typeRecord, buffer );
				writeDynamicRecord( buffer, typeRecord );
			}
		}
		
		static Command readCommand( NeoStore neoStore, FileChannel fileChannel, 
			ByteBuffer buffer ) throws IOException
		{
			// id+in_use(byte)+type_blockId(int)+nr_type_records(int)
			buffer.clear(); buffer.limit( 13 );
			if ( fileChannel.read( buffer ) != buffer.limit() )
			{
				return null;
			}
			buffer.flip();
			int id = buffer.getInt();
			byte inUseFlag = buffer.get();
			boolean inUse = false;
			if ( ( inUseFlag & Record.IN_USE.byteValue() ) == 
				Record.IN_USE.byteValue() )
			{
				inUse = true;
			}
			else if ( inUseFlag != Record.NOT_IN_USE.byteValue() )
			{
				throw new IOException( "Illegal in use flag: " + inUseFlag );
			}
			RelationshipTypeRecord record = new RelationshipTypeRecord( id );
			record.setInUse( inUse );
			record.setTypeBlock( buffer.getInt() );
			int nrTypeRecords = buffer.getInt();
			for ( int i = 0; i < nrTypeRecords; i++ )
			{
				DynamicRecord dr = readDynamicRecord( fileChannel, buffer );
				if ( dr == null )
				{
					return null;
				}
				record.addTypeRecord( dr );
			}
			return new RelationshipTypeCommand( 
				neoStore.getRelationshipTypeStore(), record );
		}

		@Override
		public boolean equals( Object o )
		{
			if ( !( o instanceof RelationshipTypeCommand ) )
			{
				return false;
			}
			return getKey() == ((Command) o).getKey();
//			return getKey().equals( ( 
//				( RelationshipTypeCommand ) o ).getKey() );
		}
	}
	
	static Command readCommand( NeoStore neoStore, FileChannel fileChannel, 
			ByteBuffer buffer ) throws IOException
	{
		buffer.clear(); buffer.limit( 1 );
		if ( fileChannel.read( buffer ) != buffer.limit() )
		{
			return null;
		}
		buffer.flip();
		byte commandType = buffer.get();
		switch ( commandType )
		{
			case NODE_COMMAND: return NodeCommand.readCommand( 
				neoStore, fileChannel, buffer ); 
			case PROP_COMMAND: return PropertyCommand.readCommand( 
				neoStore, fileChannel, buffer );
			case PROP_INDEX_COMMAND: return PropertyIndexCommand.readCommand( 
				neoStore, fileChannel, buffer );
			case REL_COMMAND: return RelationshipCommand.readCommand( 
				neoStore, fileChannel, buffer );
			case REL_TYPE_COMMAND: return RelationshipTypeCommand.readCommand( 
				neoStore, fileChannel, buffer );
			default:
				throw new IOException( "Unkown command type[" + 
					commandType + "]" );
		}
	}
}
