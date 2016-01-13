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
package org.neo4j.metrics.source;

import java.io.IOException;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.coreedge.raft.log.NaiveDurableRaftLog;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

import static org.neo4j.coreedge.raft.log.RaftLog.APPEND_INDEX_TAG;
import static org.neo4j.coreedge.raft.log.RaftLog.COMMIT_INDEX_TAG;
import static org.neo4j.coreedge.raft.state.term.TermStore.TERM_TAG;

@Documented(".Core Edge Metrics")
public class CoreEdgeMetrics extends LifecycleAdapter
{
    private static final String CORE_EDGE_PREFIX = "neo4j.core_edge";

    @Documented("Append index of the RAFT log")
    public static final String APPEND_INDEX = name( CORE_EDGE_PREFIX, "append_index" );
    @Documented("Commit index of the RAFT log")
    public static final String COMMIT_INDEX = name( CORE_EDGE_PREFIX, "commit_index" );
    @Documented("RAFT Term of this server")
    public static final String TERM = name( CORE_EDGE_PREFIX, "term" );

    private Monitors monitors;
    private MetricRegistry registry;

    private final RaftLogCommitIndexMetric raftLogCommitIndexMetric = new RaftLogCommitIndexMetric();
    private final RaftLogAppendIndexMetric raftLogAppendIndexMetric = new RaftLogAppendIndexMetric();
    private final RaftTermMetric raftTermMetric = new RaftTermMetric();

    public CoreEdgeMetrics( Monitors monitors, MetricRegistry registry )
    {
        this.monitors = monitors;
        this.registry = registry;
    }

    @Override
    public void start() throws Throwable
    {
        monitors.addMonitorListener( raftLogCommitIndexMetric, NaiveDurableRaftLog.class.getName(), COMMIT_INDEX_TAG );
        monitors.addMonitorListener( raftLogAppendIndexMetric, NaiveDurableRaftLog.class.getName(), APPEND_INDEX_TAG );
        monitors.addMonitorListener( raftTermMetric, NaiveDurableRaftLog.class.getName(), TERM_TAG );

        registry.register( COMMIT_INDEX, (Gauge<Long>) () -> raftLogCommitIndexMetric.commitIndex() );
        registry.register( APPEND_INDEX, (Gauge<Long>) () -> raftLogAppendIndexMetric.appendIndex() );
        registry.register( TERM, (Gauge<Long>) () -> raftTermMetric.term() );
    }

    @Override
    public void stop() throws IOException
    {
        registry.remove( COMMIT_INDEX );
        registry.remove( APPEND_INDEX );
        registry.remove( TERM );

        monitors.removeMonitorListener( raftLogCommitIndexMetric );
        monitors.removeMonitorListener( raftLogAppendIndexMetric );
        monitors.removeMonitorListener( raftTermMetric );
    }
}
