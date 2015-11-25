/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.catchup.tx.edge;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyLogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;

import static java.lang.Math.max;

import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;

public class TransactionLogCatchUpWriter implements TxPullResponseListener, AutoCloseable
{
    private final FileSystemAbstraction fs;
    private final LifeSupport life;
    private final PhysicalLogFiles logFiles;
    private final ReadOnlyLogVersionRepository logVersionRepository;
    private final TransactionLogWriter writer;

    public TransactionLogCatchUpWriter( File storeDir, FileSystemAbstraction fs, PageCache pageCache ) throws
            IOException
    {
        this.fs = fs;
        this.life = new LifeSupport();

        logFiles = new PhysicalLogFiles( storeDir, fs );
        logVersionRepository = new ReadOnlyLogVersionRepository( pageCache, storeDir );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, Long.MAX_VALUE /*don't rotate*/,
                new ReadOnlyTransactionIdStore( pageCache, storeDir ), logVersionRepository,
                new Monitors().newMonitor( PhysicalLogFile.Monitor.class ),
                new TransactionMetadataCache( 10, 100 ) ) );
        life.start();

        WritableLogChannel channel = logFile.getWriter();
        this.writer = new TransactionLogWriter( new LogEntryWriter( channel, new CommandWriter( channel ) ) );
    }

    @Override
    public void onTxReceived( TxPullResponse txPullResponse ) throws IOException
    {
        CommittedTransactionRepresentation tx = txPullResponse.tx();
        writer.append( tx.getTransactionRepresentation(), tx.getCommitEntry().getTxId() );
    }

    public void setCorrectTransactionId( long endTxId ) throws IOException
    {
        long currentLogVersion = logVersionRepository.getCurrentLogVersion();
        writer.checkPoint( new LogPosition( currentLogVersion, LOG_HEADER_SIZE ) );

        File currentLogFile = logFiles.getLogFileForVersion( currentLogVersion );
        writeLogHeader( fs, currentLogFile, currentLogVersion, max( BASE_TX_ID, endTxId ) );
    }

    @Override
    public void close()
    {
        life.stop();
        life.shutdown();
    }
}
