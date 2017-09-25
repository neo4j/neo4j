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
package org.neo4j.backup;

import java.time.Clock;

import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

public class BackupModuleResolveAtRuntime
{
    private final OutsideWorld outsideWorld;
    private final LogProvider logProvider;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final Monitors monitors;
    private final Clock clock;
    private final TransactionLogCatchUpFactory transactionLogCatchUpFactory;
    private final BackupCopyService backupCopyService;

    /**
     * Dependencies that can be resolved immediately after launching the backup tool
     *
     * @param outsideWorld filesystem and output streams that the tool interacts with
     * @param logProvider made available to subsequent dependency resolution classes
     * @param monitors will become shared across all resolved dependencies
     */
    public BackupModuleResolveAtRuntime( OutsideWorld outsideWorld, LogProvider logProvider, Monitors monitors )
    {
        this.outsideWorld = outsideWorld;
        this.logProvider = logProvider;
        this.monitors = monitors;
        this.clock = Clock.systemDefaultZone();
        this.transactionLogCatchUpFactory = new TransactionLogCatchUpFactory();
        this.fileSystemAbstraction = outsideWorld.fileSystem();
        this.backupCopyService = new BackupCopyService( outsideWorld );
    }

    public LogProvider getLogProvider()
    {
        return logProvider;
    }

    public FileSystemAbstraction getFileSystemAbstraction()
    {
        return fileSystemAbstraction;
    }

    public Monitors getMonitors()
    {
        return monitors;
    }

    public Clock getClock()
    {
        return clock;
    }

    public TransactionLogCatchUpFactory getTransactionLogCatchUpFactory()
    {
        return transactionLogCatchUpFactory;
    }

    public OutsideWorld getOutsideWorld()
    {
        return outsideWorld;
    }

    public BackupCopyService getBackupCopyService()
    {
        return backupCopyService;
    }
}
