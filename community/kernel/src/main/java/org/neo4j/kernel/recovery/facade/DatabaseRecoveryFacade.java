/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.recovery.facade;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.kernel.recovery.Recovery.performRecovery;
import static org.neo4j.kernel.recovery.facade.RecoveryFacadeMonitor.EMPTY_MONITOR;

public class DatabaseRecoveryFacade implements RecoveryFacade
{
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final DatabaseTracers tracers;
    private final Config config;
    private final MemoryTracker memoryTracker;
    private final LogProvider logProvider;

    public DatabaseRecoveryFacade( FileSystemAbstraction fs, PageCache pageCache, DatabaseTracers tracers, Config config, MemoryTracker memoryTracker,
            LogProvider logProvider )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.tracers = tracers;
        this.config = config;
        this.memoryTracker = memoryTracker;
        this.logProvider = logProvider;
    }

    @Override
    public void recovery( DatabaseLayout databaseLayout ) throws IOException
    {
        recovery( databaseLayout, EMPTY_MONITOR );
    }

    @Override
    public void recovery( DatabaseLayout databaseLayout, RecoveryFacadeMonitor monitor ) throws IOException
    {
        recovery( databaseLayout, RecoveryCriteria.ALL, monitor );
    }

    @Override
    public void recovery( DatabaseLayout databaseLayout, RecoveryCriteria recoveryCriteria, RecoveryFacadeMonitor monitor ) throws IOException
    {
        monitor.recoveryStarted();
        performRecovery( fs, pageCache, tracers, config, databaseLayout, memoryTracker, logProvider, recoveryCriteria );
        monitor.recoveryCompleted();
    }
}
