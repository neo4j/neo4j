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
package org.neo4j.consistency.checking.full;

import org.junit.Test;

import java.io.File;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class StoreProcessorTaskTest
{
    @Test
    public void singlePassShouldOnlyProcessTheStoreOnce() throws Exception
    {
        // given
        StoreProcessor singlePassProcessor = mock( StoreProcessor.class );
        when( singlePassProcessor.getStage() ).thenReturn( Stage.SEQUENTIAL_FORWARD );

        NodeStore store = mock( NodeStore.class );
        when( store.getStorageFileName() ).thenReturn( new File("node-store") );

        StoreProcessorTask<NodeRecord> task = new StoreProcessorTask<>( "nodes", Statistics.NONE, 1,
                store, null, "nodes", ProgressMonitorFactory.NONE.multipleParts( "check" ),
                CacheAccess.EMPTY,
                singlePassProcessor,
                QueueDistribution.ROUND_ROBIN );

        // when
        task.run();

        // then
        verify( singlePassProcessor ).applyFiltered( same( store ), any( ProgressListener.class ) );
    }
}
