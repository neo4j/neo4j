/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.state.term;

import org.junit.Test;

import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.log.monitoring.RaftTermMonitor;
import org.neo4j.kernel.monitoring.Monitors;

import static org.junit.Assert.assertEquals;

public class MonitoredTermStateTest
{
    @Test
    public void shouldMonitorTerm() throws Exception
    {
        // given

        Monitors monitors = new Monitors();
        StubRaftTermMonitor raftTermMonitor = new StubRaftTermMonitor();
        monitors.addMonitorListener( raftTermMonitor );

        // when
        new MonitoredTermState( new StubTermState(), monitors ).update( 7 );

        // then
        assertEquals(7, raftTermMonitor.term());
    }


    private static class StubTermState implements TermState
    {
        private long currentTerm;

        @Override
        public long currentTerm()
        {
            return currentTerm;
        }

        @Override
        public void update( long newTerm ) throws RaftStorageException
        {
            currentTerm = newTerm;
        }
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