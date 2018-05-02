/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.snapshot;

import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.JobScheduler.JobHandle;

import static org.neo4j.scheduler.JobScheduler.Groups.downloadSnapshot;

public class CoreStateDownloaderService extends LifecycleAdapter
{
    private final JobScheduler jobScheduler;
    private final CoreStateDownloader downloader;
    private final CommandApplicationProcess applicationProcess;
    private final Log log;
    private final TimeoutStrategy.Timeout downloaderPauseStrategy;
    private PersistentSnapshotDownloader currentJob;
    private JobHandle jobHandle;
    private boolean stopped;
    private Supplier<DatabaseHealth> dbHealth;
    private final Monitors monitors;

    public CoreStateDownloaderService( JobScheduler jobScheduler, CoreStateDownloader downloader,
            CommandApplicationProcess applicationProcess, LogProvider logProvider,
            TimeoutStrategy.Timeout downloaderPauseStrategy, Supplier<DatabaseHealth> dbHealth, Monitors monitors )
    {
        this.jobScheduler = jobScheduler;
        this.downloader = downloader;
        this.applicationProcess = applicationProcess;
        this.log = logProvider.getLog( getClass() );
        this.downloaderPauseStrategy = downloaderPauseStrategy;
        this.dbHealth = dbHealth;
        this.monitors = monitors;
    }

    public synchronized Optional<JobHandle> scheduleDownload( CatchupAddressProvider addressProvider )
    {
        if ( stopped )
        {
            return Optional.empty();
        }

        if ( currentJob == null || currentJob.hasCompleted() )
        {
            currentJob = new PersistentSnapshotDownloader( addressProvider, applicationProcess, downloader, log,
                    downloaderPauseStrategy, dbHealth, monitors );
            jobHandle = jobScheduler.schedule( downloadSnapshot, currentJob );
            return Optional.of( jobHandle );
        }
        return Optional.of( jobHandle );
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        stopped = true;

        if ( currentJob != null )
        {
            currentJob.stop();
        }
    }

    public synchronized Optional<JobHandle> downloadJob()
    {
        return Optional.ofNullable( jobHandle );
    }
}
