/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.core.state.snapshot;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import java.util.Optional;

import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.JobScheduler.JobHandle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.util.JobScheduler.Groups.downloadSnapshot;

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

    public CoreStateDownloaderService( JobScheduler jobScheduler, CoreStateDownloader downloader,
            CommandApplicationProcess applicationProcess,
            LogProvider logProvider,
            TimeoutStrategy.Timeout downloaderPauseStrategy )
    {
        this.jobScheduler = jobScheduler;
        this.downloader = downloader;
        this.applicationProcess = applicationProcess;
        this.log = logProvider.getLog( getClass() );
        this.downloaderPauseStrategy = downloaderPauseStrategy;
    }

    public synchronized Optional<JobHandle> scheduleDownload( LeaderLocator leaderLocator )
    {
        if ( stopped )
        {
            return Optional.empty();
        }

        if ( currentJob == null || currentJob.hasCompleted() )
        {
            currentJob = new PersistentSnapshotDownloader( leaderLocator, applicationProcess, downloader, log,
                    downloaderPauseStrategy );
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
