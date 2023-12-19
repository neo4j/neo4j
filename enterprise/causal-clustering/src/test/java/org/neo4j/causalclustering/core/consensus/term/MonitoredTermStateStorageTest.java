/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.term;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.log.monitoring.RaftTermMonitor;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.kernel.monitoring.Monitors;

import static org.junit.Assert.assertEquals;

public class MonitoredTermStateStorageTest
{
    @Test
    public void shouldMonitorTerm() throws Exception
    {
        // given
        Monitors monitors = new Monitors();
        StubRaftTermMonitor raftTermMonitor = new StubRaftTermMonitor();
        monitors.addMonitorListener( raftTermMonitor );
        TermState state = new TermState();
        MonitoredTermStateStorage monitoredTermStateStorage =
                new MonitoredTermStateStorage( new InMemoryStateStorage<>( new TermState() ), monitors );

        // when
        state.update( 7 );
        monitoredTermStateStorage.persistStoreData( state );

        // then
        assertEquals( 7, raftTermMonitor.term() );
    }

    private static class StubRaftTermMonitor implements RaftTermMonitor
    {
        private long term;

        @Override
        public long term()
        {
            return term;
        }

        @Override
        public void term( long term )
        {
            this.term = term;
        }
    }
}
