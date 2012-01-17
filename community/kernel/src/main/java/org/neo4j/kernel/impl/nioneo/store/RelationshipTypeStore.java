/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;

/**
 * Implementation of the relationship type store. Uses a dynamic store to store
 * relationship type names.
 */
public class RelationshipTypeStore extends AbstractNameStore<RelationshipTypeRecord>
{
    public static final String TYPE_DESCRIPTOR = "RelationshipTypeStore";
    private static final int RECORD_SIZE = 1/*inUse*/ + 4/*nameId*/;

    public RelationshipTypeStore( String fileName, Map<?,?> config )
    {
        super( fileName, config, IdType.RELATIONSHIP_TYPE );
    }

    @Override
    public void accept( RecordStore.Processor processor, RelationshipTypeRecord record )
    {
        processor.processRelationshipType( this, record );
    }

    /**
     * Creates a new relationship type store contained in <CODE>fileName</CODE>
     * If filename is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     *
     * @param fileName
     *            File name of the new relationship type store
     * @throws IOException
     *             If unable to create store or name null
     */
    public static void createStore( String fileName, Map<?, ?> config )
    {
        IdGeneratorFactory idGeneratorFactory = (IdGeneratorFactory) config.get(
                IdGeneratorFactory.class );
        FileSystemAbstraction fileSystem = (FileSystemAbstraction) config.get( FileSystemAbstraction.class );
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR ), idGeneratorFactory,
                fileSystem );
        DynamicStringStore.createStore( fileName + ".names",
            NAME_STORE_BLOCK_SIZE, idGeneratorFactory, fileSystem, IdType.RELATIONSHIP_TYPE_BLOCK );
        RelationshipTypeStore store = new RelationshipTypeStore( fileName, config );
        store.close();
    }

    void markAsReserved( int id )
    {
        PersistenceWindow window = acquireWindow( id, OperationType.WRITE );
        try
        {
            markAsReserved( id, window );
        }
        finally
        {
            releaseWindow( window );
        }
    }

    private void markAsReserved( int id, PersistenceWindow window )
    {
        Buffer buffer = window.getOffsettedBuffer( id );
        buffer.put( Record.IN_USE.byteValue() ).putInt(
            Record.RESERVED.intValue() );
    }

    @Override
    protected void rebuildIdGenerator()
    {
        logger.fine( "Rebuilding id generator for[" + getStorageFileName()
            + "] ..." );
        closeIdGenerator();
        File file = new File( getStorageFileName() + ".id" );
        if ( file.exists() )
        {
            boolean success = file.delete();
            assert success;
        }
        createIdGenerator( getStorageFileName() + ".id" );
        openIdGenerator( false );
        FileChannel fileChannel = getFileChannel();
        long highId = -1;
        int recordSize = getRecordSize();
        try
        {
            long fileSize = fileChannel.size();
            ByteBuffer byteBuffer = ByteBuffer.wrap( new byte[recordSize] );
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
                // nextId();
            }
            highId++;
            fileChannel.truncate( highId * recordSize );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                "Unable to rebuild id generator " + getStorageFileName(), e );
        }
        setHighId( highId );
        logger.fine( "[" + getStorageFileName() + "] high id=" + getHighId() );
        closeIdGenerator();
        openIdGenerator( false );
    }

    @Override
    protected RelationshipTypeRecord newRecord( int id )
    {
        return new RelationshipTypeRecord( id );
    }

    @Override
    protected IdType getNameIdType()
    {
        return IdType.RELATIONSHIP_TYPE_BLOCK;
    }

    @Override
    protected String getNameStorePostfix()
    {
        return ".names";
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }
}