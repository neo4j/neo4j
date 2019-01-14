/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Test;

import org.neo4j.logging.LogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class EntryBasedLogPruningStrategyTest extends PruningStrategyTest
{
    @Test
    public void indexToKeepTest()
    {
        // given
        files = createSegmentFiles( 10 );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 6, mock( LogProvider.class ) );

        // when
        long indexToKeep = strategy.getIndexToKeep( segments );

        // then
        assertEquals( 2, indexToKeep );
    }

    @Test
    public void pruneStrategyExceedsNumberOfEntriesTest()
    {
        //given
        files = createSegmentFiles( 10 ).subList( 5, 10 );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 7, mock( LogProvider.class ) );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        assertEquals( 4, indexToKeep );
    }

    @Test
    public void onlyFirstActiveLogFileTest()
    {
        //given
        files = createSegmentFiles( 1 );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 6, mock( LogProvider.class ) );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        assertEquals( -1, indexToKeep );
    }

    @Test
    public void onlyOneActiveLogFileTest()
    {
        //given
        files = createSegmentFiles( 6 ).subList( 4, 6 );
        EntryBasedLogPruningStrategy strategy = new EntryBasedLogPruningStrategy( 6, mock( LogProvider.class ) );

        //when
        long indexToKeep = strategy.getIndexToKeep( segments );

        //then
        assertEquals( 3, indexToKeep );
    }
}
