/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.neo4j.impl.core.RawPropertyIndex;

/**
 * Implementation of the node store.
 */
public class PropertyIndexStore extends AbstractStore implements Store
{
	// store version, should end with this string (byte encoded)
	private static final String VERSION = "PropertyIndex v0.9.3";
	private static final int KEY_STORE_BLOCK_SIZE = 30;
	 
	// in_use(byte)+prop_count(int)+key_block_id(int)
	private static final int RECORD_SIZE = 9;
	 
	private DynamicStringStore keyPropertyStore;
	
	public PropertyIndexStore( String fileName, Map<?,?> config ) 
		throws IOException
	{
		super( fileName, config );
	}

	public PropertyIndexStore( String fileName ) 
		throws IOException
	{
		super( fileName );
	}

	protected void initStorage() throws IOException
	{
		keyPropertyStore = new DynamicStringStore( 
			getStorageFileName() + ".keys", getConfig() );
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
	public void makeStoreOk() throws IOException
	{
		keyPropertyStore.makeStoreOk();
		super.makeStoreOk();
	}
	
	public void freeBlockId( int id ) throws IOException
	{
		keyPropertyStore.freeBlockId( id );
	}
	
	@Override
	protected void closeStorage() throws IOException
	{
		keyPropertyStore.close();
		keyPropertyStore = null;
	}
	
//	@Override
//	public void flush( int txIdentifier ) throws IOException
//	{
//		keyPropertyStore.flush( txIdentifier );
//		super.flush( txIdentifier );
//	}
	
	@Override
	public void flushAll() throws IOException
	{
		keyPropertyStore.flushAll();
		super.flushAll();
	}
	
	public static void createStore( String fileName ) 
		throws IOException
	{
		createEmptyStore( fileName, VERSION );
		DynamicStringStore.createStore( fileName + ".keys", 
			KEY_STORE_BLOCK_SIZE );
	}
	
	public RawPropertyIndex[] getPropertyIndexes( int count )
		throws IOException
	{
		LinkedList<RawPropertyIndex> indexList = 
			new LinkedList<RawPropertyIndex>();
		int maxIdInUse = getHighestPossibleIdInUse();
		int found = 0;
		for ( int i = 0; i <= maxIdInUse && found < count; i++ )
		{
			PropertyIndexRecord record;
			try
			{
				record = getRecord( i, (ReadFromBuffer) null );
			}
			catch ( IOException e )
			{
				continue;
			}
			found++;
			indexList.add( new RawPropertyIndex( record.getId(), getStringFor( 
				record, null ) ) );
		}
		return indexList.toArray( 
			new RawPropertyIndex[ indexList.size() ] );
	}
	
			
	public PropertyIndexRecord getRecord( int id, ReadFromBuffer buffer ) 
		throws IOException
	{
		PropertyIndexRecord record;
		if ( buffer != null && !hasWindow( id ) )
		{
			buffer.makeReadyForTransfer();
			getFileChannel().transferTo( ((long) id) * RECORD_SIZE, 
				RECORD_SIZE, buffer.getFileChannel() );
			ByteBuffer buf = buffer.getByteBuffer();
			byte inUse = buf.get();
			assert inUse == Record.IN_USE.byteValue();
			record = new PropertyIndexRecord( id );
			record.setInUse( true );
			record.setPropertyCount( buf.getInt() );
			record.setKeyBlockId( buf.getInt() );
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
		Collection<DynamicRecord> keyRecords = 
			keyPropertyStore.getLightRecords( record.getKeyBlockId(), buffer );
		for ( DynamicRecord keyRecord : keyRecords )
		{
			record.addKeyRecord( keyRecord );
		}
		return record;
	}

	public PropertyIndexRecord getLightRecord( int id, ReadFromBuffer buffer ) 
		throws IOException
	{
		PropertyIndexRecord record;
		if ( buffer != null && !hasWindow( id ) )
		{
			buffer.makeReadyForTransfer();
			getFileChannel().transferTo( ((long) id) * RECORD_SIZE, 
				RECORD_SIZE, buffer.getFileChannel() );
			ByteBuffer buf = buffer.getByteBuffer();
			byte inUse = buf.get();
			assert inUse == Record.IN_USE.byteValue();
			record = new PropertyIndexRecord( id );
			record.setInUse( true );
			record.setIsLight( true );
			record.setPropertyCount( buf.getInt() );
			record.setKeyBlockId( buf.getInt() );
		}
		else
		{
			PersistenceWindow window = acquireWindow( id, OperationType.READ );
			try
			{
				record = getRecord( id, window.getBuffer() );
				record.setIsLight( true );
			}
			finally 
			{
				releaseWindow( window );
			}
		}
		return record;
	}
	
	public void updateRecord( PropertyIndexRecord record ) throws IOException
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
			for ( DynamicRecord keyRecord : record.getKeyRecords() )
			{
				keyPropertyStore.updateRecord( keyRecord );
			}
		}
	}
	
	public Collection<DynamicRecord> allocateKeyRecords( int keyBlockId, 
		char[] chars ) throws IOException
	{
		return keyPropertyStore.allocateRecords( keyBlockId, chars );
	}
	
	public int nextKeyBlockId() throws IOException
	{
		return keyPropertyStore.nextBlockId();
	}
	
	private PropertyIndexRecord getRecord( int id, Buffer buffer ) 
		throws IOException
	{
		int offset = (int) ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		boolean inUse = ( buffer.get() == Record.IN_USE.byteValue() );
		if ( !inUse )
		{
			throw new IOException( "Record[" + id + "] not in use" );
		}
		PropertyIndexRecord record = new PropertyIndexRecord( id );
		record.setInUse( inUse );
		record.setPropertyCount( buffer.getInt() );
		record.setKeyBlockId( buffer.getInt() );
		return record;
	}
	
	private boolean transferRecord( PropertyIndexRecord record ) throws IOException
	{
		int id = record.getId();
		long count = record.getTransferCount();
		FileChannel fileChannel = getFileChannel();
		fileChannel.position( id * getRecordSize() );
		if ( count != record.getFromChannel().transferTo( 
			record.getTransferStartPosition(), count, fileChannel ) )
		{
//			throw new RuntimeException( "expected " + count + 
//				" bytes transfered" );
			return false;
		}
		return true;
	}
	
	private void updateRecord( PropertyIndexRecord record, Buffer buffer )
		throws IOException
	{
		int id = record.getId();
		int offset = (int) ( id - buffer.position() ) * getRecordSize();
		buffer.setOffset( offset );
		if ( record.inUse() )
		{
			buffer.put( Record.IN_USE.byteValue() ).putInt( 
				record.getPropertyCount() ).putInt( record.getKeyBlockId() );
		}
		else
		{
			buffer.put( Record.NOT_IN_USE.byteValue() ); // .putInt( 0 ).putInt( 
				// Record.NO_NEXT_BLOCK.intValue() );
			if ( !isInRecoveryMode() )
			{
				freeId( id );
			}
		}
	}
	
	public void makeHeavy( PropertyIndexRecord record, ReadFromBuffer buffer ) 
		throws IOException
	{
		record.setIsLight( false );
		Collection<DynamicRecord> keyRecords = 
			keyPropertyStore.getRecords( record.getKeyBlockId(), buffer );
		for ( DynamicRecord keyRecord : keyRecords )
		{
			record.addKeyRecord( keyRecord );
		}
	}
	
	public String getStringFor( PropertyIndexRecord propRecord, 
		ReadFromBuffer buffer ) throws IOException
    {
		int recordToFind = propRecord.getKeyBlockId();
		Iterator<DynamicRecord> records = 
			propRecord.getKeyRecords().iterator();
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
					keyPropertyStore.makeHeavy( record, buffer );
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
				records = propRecord.getKeyRecords().iterator();
			}
		}
		StringBuffer buf = new StringBuffer();
		for ( char[] str : charList )
		{
			buf.append( str );
		}
		return buf.toString();
    }
	
	public String toString()
	{
		return "PropertyIndexStore";
	}
}
