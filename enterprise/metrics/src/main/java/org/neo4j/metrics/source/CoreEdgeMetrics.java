/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.MetricsSettings;

import static com.codahale.metrics.MetricRegistry.name;

import static org.neo4j.coreedge.raft.log.RaftLog.APPEND_INDEX_TAG;
import static org.neo4j.coreedge.raft.log.RaftLog.COMMIT_INDEX_TAG;

public class CoreEdgeMetrics extends LifecycleAdapter
{
    private static final String NAME_PREFIX = "neo4j.core_edge";
    private static final String APPEND_INDEX = name( NAME_PREFIX, "append_index" );
    private static final String COMMIT_INDEX = name( NAME_PREFIX, "commit_index" );

    private Config config;
    private Monitors monitors;
    private MetricRegistry registry;

    private final RaftLogCommitIndexMetric raftLogCommitIndexMetric = new RaftLogCommitIndexMetric();
    private final RaftLogAppendIndexMetric raftLogAppendIndexMetric = new RaftLogAppendIndexMetric();

    public CoreEdgeMetrics( Config config, Monitors monitors, MetricRegistry registry )
    {
        this.config = config;
        this.monitors = monitors;
        this.registry = registry;
    }

    @Override
    public void start() throws Throwable
    {
        if ( config.get( MetricsSettings.coreEdgeEnabled ) )
        {
            monitors.addMonitorListener( raftLogCommitIndexMetric, NaiveDurableRaftLog.class.getName(), COMMIT_INDEX_TAG );
            monitors.addMonitorListener( raftLogAppendIndexMetric, NaiveDurableRaftLog.class.getName(), APPEND_INDEX_TAG );

            registry.register( COMMIT_INDEX, (Gauge<Long>) () -> raftLogCommitIndexMetric.commitIndex() );
            registry.register( APPEND_INDEX, (Gauge<Long>) () -> raftLogAppendIndexMetric.appendIndex() );
        }
    }

    @Override
    public void stop() throws IOException
    {
        if ( config.get( MetricsSettings.coreEdgeEnabled ) )
        {
            registry.remove( COMMIT_INDEX );
            registry.remove( APPEND_INDEX );

            monitors.removeMonitorListener( raftLogCommitIndexMetric );
            monitors.removeMonitorListener( raftLogAppendIndexMetric );
        }
    }
}
