/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;

/**
 * Implementation of the relationship type store. Uses a dynamic store to store
 * relationship type names.
 */
public class RelationshipTypeTokenStore extends TokenStore<RelationshipTypeTokenRecord>
{
    public static abstract class Configuration
        extends TokenStore.Configuration
    {

    }

    public static final String TYPE_DESCRIPTOR = "RelationshipTypeStore";
    private static final int RECORD_SIZE = 1/*inUse*/ + 4/*nameId*/;

    public RelationshipTypeTokenStore(
            File fileName,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            FileSystemAbstraction fileSystemAbstraction,
            StringLogger stringLogger,
            DynamicStringStore nameStore,
            StoreVersionMismatchHandler versionMismatchHandler,
            Monitors monitors )
    {
        super( fileName, config, IdType.RELATIONSHIP_TYPE_TOKEN, idGeneratorFactory, pageCache,
                fileSystemAbstraction, stringLogger, nameStore, versionMismatchHandler, monitors );
    }

    @Override
    public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, RelationshipTypeTokenRecord record ) throws FAILURE
    {
        processor.processRelationshipTypeToken( this, record );
    }

    @Override
    // TODO: Remove this method?
    protected void rebuildIdGenerator()
    {
        stringLogger.debug( "Rebuilding id generator for[" + getStorageFileName()
            + "] ..." );
        closeIdGenerator();
        if ( fileSystemAbstraction.fileExists( new File( getStorageFileName().getPath() + ".id" )) )
        {
            boolean success = fileSystemAbstraction.deleteFile( new File( getStorageFileName().getPath() + ".id" ));
            assert success;
        }
        createIdGenerator( new File( getStorageFileName().getPath() + ".id" ));
        openIdGenerator();
        StoreChannel fileChannel = getFileChannel();
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
        stringLogger.debug( "[" + getStorageFileName() + "] high id=" + getHighId() );
        closeIdGenerator();
        openIdGenerator();
    }

    @Override
    protected RelationshipTypeTokenRecord newRecord( int id )
    {
        return new RelationshipTypeTokenRecord( id );
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
