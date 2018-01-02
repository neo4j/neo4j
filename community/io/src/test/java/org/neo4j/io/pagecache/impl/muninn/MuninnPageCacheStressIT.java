/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.muninn;

import org.junit.Test;

import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.stress.Condition;
import org.neo4j.io.pagecache.stress.PageCacheStressTest;

import static org.neo4j.io.pagecache.stress.Conditions.numberOfEvictions;

/**
 * A stress test for Muninn page cache.
 *
 * Uses @PageCacheStressTest - see details there.
 *
 * Configured to run until it sees a million evictions, which should take few minutes.
 */
public class MuninnPageCacheStressIT
{
    @Test
    public void shouldHandleTheStressOfOneMillionEvictions() throws Exception
    {
        DefaultPageCacheTracer monitor = new DefaultPageCacheTracer();
        Condition condition = numberOfEvictions( monitor, 1_000_000 );

        PageCacheStressTest runner = new PageCacheStressTest.Builder()
                .with( monitor )
                .with( condition )
                .build();

        runner.run();
    }
}
