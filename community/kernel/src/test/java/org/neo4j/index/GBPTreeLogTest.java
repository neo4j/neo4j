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

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.SimpleLongLayout;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.SilentHealth;
import org.neo4j.kernel.Health;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;

public class GBPTreeLogTest
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( logProvider );

    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();

    private void insert( GBPTree<MutableLong, MutableLong> index, long key ) throws IOException
    {
        try ( Writer<MutableLong,MutableLong> writer = index.writer() )
        {
            writer.put( new MutableLong( key ), new MutableLong( key ) );
        }
    }

    private GBPTree<MutableLong,MutableLong> gbpTree( Log log )
            throws java.io.IOException
    {
        return gbpTree( log, new SilentHealth() );
    }

    private GBPTree<MutableLong,MutableLong> gbpTree( Log log, Health health )
            throws java.io.IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( fs.get() );
        File file = directory.file( "index" );
        return new GBPTree<>( pageCache, file, layout, 0, NO_MONITOR, NO_HEADER, health, log );
    }

    @Test
    public void shouldLogMessageDuringCloseWithCheckpointAndContent() throws Exception
    {
        // GIVEN
        Log log = logProvider.getLog( GBPTree.class );

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index = gbpTree( log ) )
        {
            insert( index, 1 );
        }

        // THEN
        logProvider.assertContainsLogCallContaining( "Did checkpoint during close, had anything to write? true" );
    }

    @Test
    public void shouldLogMessageDuringCloseWithCheckpointAndNoContent() throws Exception
    {
        // GIVEN
        Log log = logProvider.getLog( GBPTree.class );

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index = gbpTree( log ) )
        {
        }

        // THEN
        logProvider.assertContainsLogCallContaining( "Did checkpoint during close, had anything to write? false" );
    }

    @Test
    public void shouldLogMessageDuringCloseWithoutCheckpoint() throws Exception
    {
        // GIVEN
        AtomicBoolean isHealthy = new AtomicBoolean( true );
        Health health = new ControllableHealth( isHealthy );
        Log log = logProvider.getLog( GBPTree.class );

        // WHEN
        try ( GBPTree<MutableLong,MutableLong> index = gbpTree( log, health ) )
        {
            isHealthy.set( false );
        }
        logProvider.assertContainsLogCallContaining( "Did not checkpoint during close" );
    }
}
