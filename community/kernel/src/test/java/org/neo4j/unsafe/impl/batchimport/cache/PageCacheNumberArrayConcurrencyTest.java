/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.function.BiFunction;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.Race;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;

public abstract class PageCacheNumberArrayConcurrencyTest
{
    protected static final int COUNT = 100;
    protected static final int LAPS = 2_000;
    protected static final int CONTESTANTS = 10;

    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory dir = TestDirectory.testDirectory();
    protected final RandomRule random = new RandomRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fs ).around( dir ).around( random ).around( pageCacheRule );

    @Test
    public void shouldHandleConcurrentAccessToSameData() throws Throwable
    {
        doRace( this::wholeFileRacer );
    }

    @Test
    public void shouldHandleConcurrentAccessToDifferentData() throws Throwable
    {
        doRace( this::fileRangeRacer );
    }

    private void doRace( BiFunction<NumberArray,Integer,Runnable> contestantCreator ) throws Throwable
    {
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        PagedFile file = pageCache.map( dir.file( "file" ), pageCache.pageSize(), CREATE, DELETE_ON_CLOSE );
        Race race = new Race();
        try ( NumberArray array = getNumberArray( file ) )
        {
            for ( int i = 0; i < CONTESTANTS; i++ )
            {
                race.addContestant( contestantCreator.apply( array, i ) );
            }
            race.go();

        }
    }

    protected abstract Runnable fileRangeRacer( NumberArray array, int contestant );

    protected abstract NumberArray getNumberArray( PagedFile file ) throws IOException;

    protected abstract Runnable wholeFileRacer( NumberArray array, int contestant );

}
