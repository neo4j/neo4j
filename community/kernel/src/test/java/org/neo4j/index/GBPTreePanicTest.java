/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.index.internal.gbptree.CheckpointCounter;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.SimpleLongLayout;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.Health;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.rules.RuleChain.outerRule;

public class GBPTreePanicTest
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule );

    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();

    @Test
    public void shouldCheckpointOnCloseIfHealthy() throws Exception
    {
        // GIVEN
        AtomicBoolean isHealthy = new AtomicBoolean( true );
        Health health = new ControllableHealth( isHealthy );
        CheckpointCounter checkpointCounter = new CheckpointCounter();

        try( GBPTree<MutableLong,MutableLong> index = gbpTree( health, checkpointCounter ) )
        {
            // WHEN
            checkpointCounter.reset();
            assertThat( checkpointCounter.count(), is( 0 ) );
            insert( index, 1 );
            index.checkpoint( IOLimiter.unlimited() );
            assertThat( checkpointCounter.count(), is(1 ) );
            insert( index, 2 );

        }
        // THEN, after close
        assertThat( checkpointCounter.count(), is( 2 ) );
    }

    @Test
    public void shouldNotCheckpointOnCloseIfNotHealthy() throws Exception
    {
        // GIVEN
        AtomicBoolean isHealthy = new AtomicBoolean( true );
        Health health = new ControllableHealth( isHealthy );
        CheckpointCounter checkpointCounter = new CheckpointCounter();

        try ( GBPTree<MutableLong,MutableLong> index = gbpTree( health, checkpointCounter ) )
        {
            // WHEN
            checkpointCounter.reset();
            assertThat( checkpointCounter.count(), is( 0 ) );
            insert( index, 1 );
            index.checkpoint( IOLimiter.unlimited() );
            assertThat( checkpointCounter.count(), is( 1 ) );
            insert( index, 2 );
            isHealthy.set( false );

        }
        // THEN, after close
        assertThat( checkpointCounter.count(), is( 1 ) );
    }

    private void insert( GBPTree<MutableLong, MutableLong> index, long key ) throws IOException
    {
        try ( Writer<MutableLong,MutableLong> writer = index.writer() )
        {
            writer.put( new MutableLong( key ), new MutableLong( key ) );
        }
    }

    private GBPTree<MutableLong,MutableLong> gbpTree( Health health, CheckpointCounter monitor )
            throws java.io.IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( fs.get() );
        File file = directory.file( "index" );
        return new GBPTree<>( pageCache, file, layout, 0, monitor, GBPTree.NO_HEADER, health );
    }

    private class ControllableHealth implements Health
    {
        private final AtomicBoolean isHealthy;

        ControllableHealth( AtomicBoolean isHealthy )
        {
            this.isHealthy = isHealthy;
        }

        @Override
        public boolean isHealthy()
        {
            return isHealthy.get();
        }
    }
}
