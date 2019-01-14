/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache.TransactionMetadata;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;

/**
 * Used for reading transactions off of file.
 */
public class ReadOnlyTransactionStore implements Lifecycle, LogicalTransactionStore
{
    private final LifeSupport life = new LifeSupport();
    private final LogicalTransactionStore physicalStore;

    public ReadOnlyTransactionStore( PageCache pageCache, FileSystemAbstraction fs, File fromPath, Config config,
            Monitors monitors ) throws IOException
    {
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 100 );
        LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        LogFiles logFiles = LogFilesBuilder
                .activeFilesBuilder( fromPath, fs, pageCache ).withLogEntryReader( logEntryReader )
                .withConfig( config )
                .build();
        physicalStore = new PhysicalLogicalTransactionStore( logFiles, transactionMetadataCache, logEntryReader,
                monitors, true );
    }

    @Override
    public TransactionCursor getTransactions( long transactionIdToStartFrom )
            throws IOException
    {
        return physicalStore.getTransactions( transactionIdToStartFrom );
    }

    @Override
    public TransactionCursor getTransactions( LogPosition position ) throws IOException
    {
        return physicalStore.getTransactions( position );
    }

    @Override
    public TransactionCursor getTransactionsInReverseOrder( LogPosition backToPosition ) throws IOException
    {
        return physicalStore.getTransactionsInReverseOrder( backToPosition );
    }

    @Override
    public TransactionMetadata getMetadataFor( long transactionId ) throws IOException
    {
        return physicalStore.getMetadataFor( transactionId );
    }

    @Override
    public void init()
    {
        life.init();
    }

    @Override
    public void start()
    {
        life.start();
    }

    @Override
    public void stop()
    {
        life.stop();
    }

    @Override
    public void shutdown()
    {
        life.shutdown();
    }
}
