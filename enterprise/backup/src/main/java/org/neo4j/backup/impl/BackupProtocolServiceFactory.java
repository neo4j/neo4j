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
package org.neo4j.backup.impl;

import java.io.OutputStream;
import java.util.function.Supplier;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.util.Objects.requireNonNull;
import static org.neo4j.backup.impl.BackupPageCacheContainer.of;
import static org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.logging.FormattedLogProvider.toOutputStream;

public final class BackupProtocolServiceFactory
{

    private BackupProtocolServiceFactory()
    {
    }

    public static BackupProtocolService backupProtocolService()
    {
        return backupProtocolService( System.out );
    }

    public static BackupProtocolService backupProtocolService( OutputStream logDestination )
    {
        JobScheduler scheduler = createInitialisedScheduler();
        PageCache pageCache = createPageCache( new DefaultFileSystemAbstraction(), scheduler );
        BackupPageCacheContainer pageCacheContainer = of( pageCache, scheduler );
        return backupProtocolService( DefaultFileSystemAbstraction::new, toOutputStream( logDestination ), logDestination, new Monitors(), pageCacheContainer );
    }

    public static BackupProtocolService backupProtocolService( Supplier<FileSystemAbstraction> fileSystemSupplier, LogProvider logProvider,
            OutputStream logDestination, Monitors monitors, PageCache pageCache )
    {
        return backupProtocolService( fileSystemSupplier, logProvider, logDestination, monitors, of( pageCache ) );
    }

    private static BackupProtocolService backupProtocolService( Supplier<FileSystemAbstraction> fileSystemSupplier, LogProvider logProvider,
            OutputStream logDestination, Monitors monitors, BackupPageCacheContainer pageCacheContainer )
    {
        requireNonNull( fileSystemSupplier );
        requireNonNull( logProvider );
        requireNonNull( logDestination );
        requireNonNull( monitors );
        requireNonNull( pageCacheContainer );
        return new BackupProtocolService( fileSystemSupplier, logProvider, logDestination, monitors, pageCacheContainer );
    }
}
