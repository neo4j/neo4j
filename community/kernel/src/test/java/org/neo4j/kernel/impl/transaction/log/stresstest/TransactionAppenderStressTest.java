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
package org.neo4j.kernel.impl.transaction.log.stresstest;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.BooleanSupplier;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.stresstest.workload.Runner;
import org.neo4j.test.TargetDirectory;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;

public class TransactionAppenderStressTest
{
    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void concurrentTransactionAppendingTest() throws Exception
    {
        int threads = 10;
        File workingDirectory = directory.directory( "work" );
        Callable<Long> runner = new Builder()
                .with( Builder.untilTimeExpired( 10, SECONDS ) )
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

        public static BooleanSupplier untilTimeExpired( long duration, TimeUnit unit )
        {
            final long endTimeInMilliseconds = currentTimeMillis() + unit.toMillis( duration );
            return new BooleanSupplier()
            {
                @Override
                public boolean getAsBoolean()
                {
                    return currentTimeMillis() <= endTimeInMilliseconds;
                }
            };
        }

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
        private File workingDirectory;

        public TransactionIdChecker( File workingDirectory )
        {
            this.workingDirectory = workingDirectory;
        }

        public long parseAllTxLogs() throws IOException
        {
            FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
            long txId = -1;
            try ( ReadableLogChannel channel = openLogFile( fs, 0 ) )
            {
                LogEntryReader<ReadableLogChannel> reader =
                        new VersionAwareLogEntryReader<>( LogEntryVersion.CURRENT.byteCode() );
                LogEntry logEntry = reader.readLogEntry( channel );
                for (; logEntry != null; logEntry = reader.readLogEntry( channel ) )
                {
                    if ( logEntry.getType() == LogEntryByteCodes.TX_1P_COMMIT )
                    {
                        txId = logEntry.<OnePhaseCommit>as().getTxId();
                    }
                }
            }
            return txId;
        }

        private ReadableLogChannel openLogFile( FileSystemAbstraction fs, int version ) throws IOException
        {
            PhysicalLogFiles logFiles = new PhysicalLogFiles( workingDirectory, fs );
            PhysicalLogVersionedStoreChannel channel = PhysicalLogFile.openForVersion( logFiles, fs, version );
            return new ReadAheadLogChannel( channel, new ReaderLogVersionBridge( fs, logFiles ),
                    DEFAULT_READ_AHEAD_SIZE );
        }
    }
}
