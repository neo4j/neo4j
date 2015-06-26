/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NeoStore.Position;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.storemigration.StoreFileType;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.store.NeoStore.RECORD_SIZE;

/**
 * Reads the contents of a {@link NeoStore neostore} store. Namely all of its {@link Position records}
 * and makes those {@link #getValue(Position) available} for viewing.
 */
public class NeoStoreUtil
{
    private final Map<Position,Long> values = new HashMap<>();

    public static boolean neoStoreExists( PageCache pageCache, File storeDir )
    {
        try ( PagedFile file = pageCache.map( neoStoreFile( storeDir, StoreFileType.STORE ), pageCache.pageSize() ) )
        {
            if ( file.getLastPageId() == -1 )
            {
                return false;
            }
        }
        catch ( IOException e )
        {
            return false;
        }

        return true;
    }

    public NeoStoreUtil( File storeDir, PageCache pageCache )
    {
        File neoStoreFile = neoStoreFile( storeDir, StoreFileType.STORE );
        try ( PagedFile pagedFile = pageCache.map( neoStoreFile, pageCache.pageSize() ) )
        {
            if ( pagedFile.getLastPageId() != -1 )
            {
                try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_LOCK ) )
                {
                    if ( cursor.next() )
                    {
                        byte[] data = new byte[Position.values().length * RECORD_SIZE];
                        do
                        {
                            cursor.getBytes( data );
                        }
                        while ( cursor.shouldRetry() );
                        ByteBuffer buf = ByteBuffer.wrap( data );
                        int pos = 0;
                        while ( pos < Position.values().length && buf.remaining() >= RECORD_SIZE )
                        {
                            if ( buf.get() == Record.IN_USE.byteValue() )
                            {
                                values.put( Position.values()[pos], buf.getLong() );
                            }
                            else
                            {
                                buf.getLong();
                            }
                            pos++;
                        }
                    }
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Returns the record value for the given {@link Position position}.
     *
     * @param position record to return value for.
     * @return the {@code long} value read from the record specified by {@code position}.
     * @throws IllegalStateException if the neostore that these values were read from didn't have
     * the record specified by {@link Position position}.
     */
    public long getValue( Position position )
    {
        Long value = values.get( position );
        if ( value == null )
        {
            throw new IllegalStateException( "Wanted record " + position +
                    ", but this record wasn't read since the neostore didn't contain it" );
        }
        return value.longValue();
    }

    public long getCreationTime()
    {
        return getValue( Position.TIME );
    }

    public long getStoreId()
    {
        return getValue( Position.RANDOM_NUMBER );
    }

    public long getLastCommittedTx()
    {
        return getValue( Position.LAST_TRANSACTION_ID );
    }

    public long getLogVersion()
    {
        return getValue( Position.LOG_VERSION );
    }

    public long getStoreVersion()
    {
        return getValue( Position.STORE_VERSION );
    }

    public long getFirstGraphProp()
    {
        return getValue( Position.FIRST_GRAPH_PROPERTY );
    }

    public long getLastCommittedTxChecksum()
    {
        return getValue( Position.LAST_TRANSACTION_CHECKSUM );
    }

    public long getLastCommittedTxLogVersion()
    {
        return getValue( Position.LAST_TRANSACTION_LOG_VERSION );
    }

    public long getLastCommittedTxLogByteOffset()
    {
        return getValue( Position.LAST_TRANSACTION_LOG_BYTE_OFFSET );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( "Neostore contents:%n" );
        int i = 0;
        for ( Position position : Position.values() )
        {
            Long value = values.get( position );
            if ( value != null )
            {
                builder.append( i++ ).append( ": " ).append( position.description() )
                       .append( ": " ).append( value ).append( "%n" );
            }
        }
        builder.append( "=> store id: " )
               .append( new StoreId( getCreationTime(), getStoreId(), getStoreVersion(), -1, -1 ) );
        return format( builder.toString() );
    }

    public static boolean storeExists( File storeDir )
    {
        return storeExists( storeDir, new DefaultFileSystemAbstraction() );
    }

    public static boolean storeExists( File storeDir, FileSystemAbstraction fs )
    {
        return fs.fileExists( neoStoreFile( storeDir, StoreFileType.STORE ) );
    }

    private static File neoStoreFile( File storeDir, StoreFileType type )
    {
        return new File( storeDir, type.augment( NeoStore.DEFAULT_NAME ) );
    }
}
