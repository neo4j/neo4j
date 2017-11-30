/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.POOLED;

public class CoreStateDownloaderService
{
    static final String OPERATION_NAME = "download of snapshot";

    private final JobScheduler jobScheduler;
    private final CoreStateDownloader downloader;
    private final CommandApplicationProcess applicationProcess;
    private final Log log;
    private PersistentSnapshotDownloader currentJob = null;
    private final JobScheduler.Group downloadSnapshotGroup;

    public CoreStateDownloaderService( JobScheduler jobScheduler, CoreStateDownloader downloader,
            CommandApplicationProcess applicationProcess,
            LogProvider logProvider )
    {
        this.jobScheduler = jobScheduler;
        this.downloader = downloader;
        this.applicationProcess = applicationProcess;
        this.log = logProvider.getLog( getClass() );
        this.downloadSnapshotGroup = new JobScheduler.Group( "download snapshot", POOLED );
    }

    public void scheduleDownload( LeaderLocator leaderLocator )
    {
        if ( currentJob == null || currentJob.hasCompleted() )
        {
            synchronized ( this )
            {
                if ( currentJob == null || currentJob.hasCompleted() )
                {
                    currentJob = new PersistentSnapshotDownloader( leaderLocator, applicationProcess, downloader, log );
                    jobScheduler.schedule( downloadSnapshotGroup, currentJob );
                }
            }
        }
    }
}
