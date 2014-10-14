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
import org.neo4j.kernel.impl.api.CountsKey;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader;
import org.neo4j.register.Register;

public class CountsStore extends SortedKeyValueStore<CountsKey, Register.LongRegister>
{
    static final CountsRecordSerializer RECORD_SERIALIZER = new CountsRecordSerializer();
    static final CountsStoreWriter.Factory WRITER_FACTORY = new CountsStoreWriter.Factory();

    public CountsStore( FileSystemAbstraction fs, PageCache pageCache, File file, PagedFile pages,
                        SortedKeyValueStoreHeader header )
    {
        super( fs, pageCache, file, pages, header, RECORD_SERIALIZER, WRITER_FACTORY );
    }

    public static void createEmpty( PageCache pageCache, File storeFile, String version )
    {
        try
        {
            PagedFile pages = mapCountsStore( pageCache, storeFile );
            try
            {
                SortedKeyValueStoreHeader.empty( version ).write( pages );
            }
            finally
            {
                pages.flush();
                pageCache.unmap( storeFile );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    public static CountsStore open( FileSystemAbstraction fs, PageCache pageCache, File storeFile )
            throws IOException
    {
        PagedFile pages = mapCountsStore( pageCache, storeFile );
        SortedKeyValueStoreHeader header = SortedKeyValueStoreHeader.read( pages );
        return new CountsStore( fs, pageCache, storeFile, pages, header );
    }

    private static PagedFile mapCountsStore( PageCache pageCache, File storeFile ) throws IOException
    {
        int pageSize = pageCache.pageSize() - (pageCache.pageSize() % RECORD_SIZE);
        return pageCache.map( storeFile, pageSize );
    }
}
