/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache.TransactionMetadata;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

/**
 * Used for reading transactions off of file.
 */
public class ReadOnlyTransactionStore extends LifecycleAdapter implements LogicalTransactionStore
{
    private final LifeSupport life = new LifeSupport();
    private final LogicalTransactionStore physicalStore;

    public ReadOnlyTransactionStore( PageCache pageCache, FileSystemAbstraction fs, File fromPath, Monitors monitors )
            throws IOException
    {
        PhysicalLogFiles logFiles = new PhysicalLogFiles( fromPath, fs );
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 10, 100 );
        final ReadOnlyTransactionIdStore transactionIdStore = new ReadOnlyTransactionIdStore( pageCache, fromPath );
        PhysicalLogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 0,
                transactionIdStore, new ReadOnlyLogVersionRepository( pageCache, fromPath ),
                monitors.newMonitor( PhysicalLogFile.Monitor.class ), transactionMetadataCache ) );
        physicalStore = new PhysicalLogicalTransactionStore( logFile, transactionMetadataCache );
    }

    @Override
    public TransactionCursor getTransactions( long transactionIdToStartFrom )
            throws NoSuchTransactionException, IOException
    {
        return physicalStore.getTransactions( transactionIdToStartFrom );
    }

    @Override
    public TransactionCursor getTransactions( LogPosition position ) throws NoSuchTransactionException, IOException
    {
        return physicalStore.getTransactions( position );
    }

    @Override
    public TransactionMetadata getMetadataFor( long transactionId ) throws NoSuchTransactionException, IOException
    {
        return physicalStore.getMetadataFor( transactionId );
    }

    @Override
    public void init() throws Throwable
    {
        life.init();
    }

    @Override
    public void start() throws Throwable
    {
        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }
}
