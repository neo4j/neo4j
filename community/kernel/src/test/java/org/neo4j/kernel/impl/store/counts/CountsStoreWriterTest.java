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

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.api.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.api.CountsKey.relationshipKey;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsKey;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.AbstractStore;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

public class CountsStoreWriterTest
{
    @Test
    public void shouldWriteEntriesToTheCountsStore() throws IOException
    {
        // given
        final CountsStoreWriter writer = new CountsStoreWriter( fs, pageCache, emptyHeader, file, lastTxId );

        // when
        writer.visit( nodeKey( 0 ), 42 );
        writer.visit( relationshipKey( 1, 2, 3 ), 24 );
        writer.close();

        // then
        try
        {
            final CountsStore counts = writer.openForReading();

            assertEquals( lastTxId, counts.lastTxId() );
            assertEquals( 2, counts.totalRecordsStored() );
            assertEquals( file, counts.file() );
            counts.accept( new RecordVisitor()
            {
                @Override
                public void visit( CountsKey key, long value )
                {
                    key.accept( new CountsVisitor()
                    {
                        @Override
                        public void visitNodeCount( int labelId, long count )
                        {
                            assertEquals( 0, labelId );
                            assertEquals( 42, count );
                        }

                        @Override
                        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
                        {
                            assertEquals( 1, startLabelId );
                            assertEquals( 2, typeId );
                            assertEquals( 3, endLabelId );
                            assertEquals( 24, count );
                        }
                    }, value );
                }
            } );

        }
        finally
        {
            pageCache.unmap( file );
        }
    }


    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    private final CountsStoreHeader emptyHeader = CountsStoreHeader.empty( AbstractStore.ALL_STORES_VERSION );
    private final File file = new File( "file" );
    private final long lastTxId = 100;
    private FileSystemAbstraction fs;
    private PageCache pageCache;

    @Before
    public void setup()
    {
        fs = fsRule.get();
        pageCache = pageCacheRule.getPageCache( fs, new Config() );
    }
}
