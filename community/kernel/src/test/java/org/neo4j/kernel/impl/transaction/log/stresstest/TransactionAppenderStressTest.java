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
package org.neo4j.kernel.impl.transaction.log.stresstest;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.function.BooleanSupplier;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.stresstest.workload.Runner;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.function.Suppliers.untilTimeExpired;

public class TransactionAppenderStressTest
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory( );

    @Test
    public void concurrentTransactionAppendingTest() throws Exception
    {
        int threads = 10;
        File workingDirectory = directory.directory( "work" );
        Callable<Long> runner = new Builder()
                .with( untilTimeExpired( 10, SECONDS ) )
                .withWorkingDirectory( workingDirectory )
                .withNumThreads( threads )
                .build();

        long appendedTxs = runner.call();

        assertEquals( new TransactionIdChecker( workingDirectory ).parseAllTxLogs(), appendedTxs );
    }

    public static class Builder
    {
        private BooleanSupplier condition;
        private File workingDirectory;
        private int threads;

        public Builder with( BooleanSupplier condition )
        {
            this.condition = condition;
            return this;
        }

        public Builder withWorkingDirectory( File workingDirectory )
        {
            this.workingDirectory = workingDirectory;
            return this;
        }

        public Builder withNumThreads( int threads )
        {
            this.threads = threads;
            return this;
        }

        public Callable<Long> build()
        {
            return new Runner( workingDirectory, condition, threads );
        }
    }

    public static class TransactionIdChecker
    {
        private final File workingDirectory;

        public TransactionIdChecker( File workingDirectory )
        {
            this.workingDirectory = workingDirectory;
        }

        public long parseAllTxLogs() throws IOException
        {
            // Initialize this txId to the BASE_TX_ID because if we don't find any tx log that means that
            // no transactions have been appended in this test and that getLastCommittedTransactionId()
            // will also return this constant. Why this is, is another question - but thread scheduling and
            // I/O spikes on some build machines can be all over the place and also the test duration is
            // configurable.
            long txId = TransactionIdStore.BASE_TX_ID;

            try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
                  ReadableLogChannel channel = openLogFile( fs, 0 ) )
            {
                LogEntryReader<ReadableLogChannel> reader = new VersionAwareLogEntryReader<>();
                LogEntry logEntry = reader.readLogEntry( channel );
                for ( ; logEntry != null; logEntry = reader.readLogEntry( channel ) )
                {
                    if ( logEntry.getType() == LogEntryByteCodes.TX_COMMIT )
                    {
                        txId = logEntry.<LogEntryCommit>as().getTxId();
                    }
                }
            }
            return txId;
        }

        private ReadableLogChannel openLogFile( FileSystemAbstraction fs, int version ) throws IOException
        {
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( workingDirectory, fs ).build();
            PhysicalLogVersionedStoreChannel channel = logFiles.openForVersion( version );
            return new ReadAheadLogChannel( channel, new ReaderLogVersionBridge( logFiles ) );
        }
    }
}
