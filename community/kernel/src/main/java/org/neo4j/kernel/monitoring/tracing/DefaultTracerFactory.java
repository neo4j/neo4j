/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.monitoring.tracing;

import java.time.Clock;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.version.DefaultVersionStorageTracer;
import org.neo4j.io.pagecache.tracing.version.VersionStorageTracer;
import org.neo4j.kernel.impl.api.tracer.DefaultTracer;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.logging.InternalLog;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

/**
 * The default TracerFactory, when nothing else is otherwise configured.
 */
@ServiceProvider
public class DefaultTracerFactory implements TracerFactory {
    @Override
    public String getName() {
        return "default";
    }

    @Override
    public PageCacheTracer createPageCacheTracer(
            Monitors monitors, JobScheduler jobScheduler, SystemNanoClock clock, InternalLog log, Config config) {
        return new DefaultPageCacheTracer(config.get(GraphDatabaseInternalSettings.per_file_metrics_counters));
    }

    @Override
    public DatabaseTracer createDatabaseTracer(PageCacheTracer pageCacheTracer, Clock clock) {
        return new DefaultTracer(pageCacheTracer);
    }

    @Override
    public VersionStorageTracer createVersionStorageTracer(PageCacheTracer pageCacheTracer, InternalLog log) {
        return new DefaultVersionStorageTracer(pageCacheTracer);
    }
}
