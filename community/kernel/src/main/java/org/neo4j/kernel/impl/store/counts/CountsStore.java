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
package org.neo4j.kernel.impl.store.counts;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader;
import org.neo4j.register.Register.CopyableDoubleLongRegister;
import org.neo4j.register.Registers;

import static org.neo4j.register.Register.LongRegister;

public class CountsStore extends SortedKeyValueStore<CountsKey, CopyableDoubleLongRegister>
{
    static final int RECORD_SIZE /*bytes*/ = 16 /*key*/ + 16 /*value*/;
    static final CountsRecordSerializer RECORD_SERIALIZER = CountsRecordSerializer.INSTANCE;
    static final CountsStoreWriter.Factory WRITER_FACTORY = new CountsStoreWriter.Factory();

    public CountsStore( FileSystemAbstraction fs, PageCache pageCache, File file, PagedFile pages,
                        SortedKeyValueStoreHeader header )
    {
        super( fs, pageCache, file, pages, header, RECORD_SERIALIZER, RECORD_SIZE, WRITER_FACTORY );
    }

    public static void createEmpty( PageCache pageCache, File storeFile, SortedKeyValueStoreHeader header )
    {
        try
        {
            try ( PagedFile pages = mapCountsStore( pageCache, storeFile ) )
            {
                header.write( pages );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    public static CountsStore open( FileSystemAbstraction fs, final PageCache pageCache, final File storeFile )
            throws IOException
    {
        PagedFile pages = mapCountsStore( pageCache, storeFile );
        try
        {
            SortedKeyValueStoreHeader header = SortedKeyValueStoreHeader.read( RECORD_SIZE, pages );
            CountsStore countsStore = new CountsStore( fs, pageCache, storeFile, pages, header );

            final LongRegister keys = Registers.newLongRegister( 0 );
            countsStore.accept( new KeyValueRecordVisitor<CountsKey,CopyableDoubleLongRegister>()
            {
                @Override
                public void visit( CountsKey key, CopyableDoubleLongRegister register )
                {
                    if ( register.hasValues( 0, 0 ) )
                    {
                        throw new UnderlyingStorageException( "Counts store contains unexpected value (0,0)" );
                    }
                    keys.increment( 1 );
                }
            }, Registers.newDoubleLongRegister() );

            if ( keys.read() != header.dataRecords() )
            {
                throw new UnderlyingStorageException( "Counts store is corrupted" );
            }

            return countsStore;
        }
        catch ( Exception e )
        {
            pages.close();
            throw e;
        }
    }

    private static PagedFile mapCountsStore( PageCache pageCache, File storeFile ) throws IOException
    {
        int pageSize = pageCache.pageSize() - (pageCache.pageSize() % RECORD_SIZE);
        return pageCache.map( storeFile, pageSize );
    }
}
