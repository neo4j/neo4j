/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index.persson;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.helpers.Format.duration;

public class NativeLabelScanStoreTest
{
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule( false );
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( getClass() );

    @Test
    public void shouldCompeteWithLucene() throws Exception
    {
        File storeDir = testDirectory.directory();
        final PageCache pageCache = pageCacheRule.getPageCache( new DefaultFileSystemAbstraction() );
        NativeLabelScanStore labelScanStore = new NativeLabelScanStore( pageCache, storeDir );
        int count = 100_000;

        long time = currentTimeMillis();
        try ( final LabelScanWriter writer = labelScanStore.newWriter() )
        {
            for ( int id = 0; id < count; id++ )
            {
                writer.write( NodeLabelUpdate.labelChanges( id, EMPTY_LONG_ARRAY, someLabels() ) );
                if ( id % 100_000 == 0 )
                {
                    System.out.println( id + " at " + ((double)id / (currentTimeMillis() - time)) + " nodes/ms" );
                }
            }
        }
        long writeTime = currentTimeMillis() - time;
        System.out.println( "write:" + duration( writeTime ) );

        time = currentTimeMillis();
        try ( final LabelScanReader reader = labelScanStore.newReader() )
        {
            for ( int labelId = 1; labelId <= 2; labelId++ )
            {
                final PrimitiveLongIterator primitiveLongIterator = reader.nodesWithLabel( labelId );
                assertThat( PrimitiveLongCollections.count( primitiveLongIterator ), equalTo( count ) );
            }
        }
        long readTime = currentTimeMillis() - time;
        System.out.println( "read:" + duration( readTime ) );

        labelScanStore.shutdown();
    }

    private long[] someLabels()
    {
        return new long[]{1L, 2L};
    }
}
