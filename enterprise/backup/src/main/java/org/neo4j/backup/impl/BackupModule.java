/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.backup.impl;

import java.time.Clock;

import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

public class BackupModule
{
    private final OutsideWorld outsideWorld;
    private final LogProvider logProvider;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final Monitors monitors;
    private final Clock clock;
    private final TransactionLogCatchUpFactory transactionLogCatchUpFactory;

    /**
     * Dependencies that can be resolved immediately after launching the backup tool
     *
     * @param outsideWorld filesystem and output streams that the tool interacts with
     * @param logProvider made available to subsequent dependency resolution classes
     * @param monitors will become shared across all resolved dependencies
     */
    BackupModule( OutsideWorld outsideWorld, LogProvider logProvider, Monitors monitors )
    {
        this.outsideWorld = outsideWorld;
        this.logProvider = logProvider;
        this.monitors = monitors;
        this.clock = Clock.systemDefaultZone();
        this.transactionLogCatchUpFactory = new TransactionLogCatchUpFactory();
        this.fileSystemAbstraction = outsideWorld.fileSystem();
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
}
