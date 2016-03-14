/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.log;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule.RaftLogImplementation;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import static org.neo4j.coreedge.raft.ReplicatedInteger.valueOf;
import static org.neo4j.coreedge.raft.log.RaftLogHelper.readLogEntry;
import static org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule.RaftLogImplementation.NAIVE;
import static org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule.RaftLogImplementation.PHYSICAL;

@RunWith(Parameterized.class)
public class RaftLogDurabilityTest
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    private final RaftLogFactory logFactory;

    public RaftLogDurabilityTest( RaftLogImplementation ignored, RaftLogFactory logFactory )
    {
        this.logFactory = logFactory;
    }

    @Parameters(name = "log:{0}")
    public static Collection<Object[]> data()
    {
        RaftLogFactory naive = ( fileSystem ) -> {
            File directory = new File( "raft-log" );
            fileSystem.mkdir( directory );
            return new NaiveDurableRaftLog( fileSystem, directory, new DummyRaftableContentSerializer(),
                    NullLogProvider.getInstance() );
        };

        RaftLogFactory physical = ( fileSystem ) -> {
            File directory = new File( "raft-log" );
            fileSystem.mkdir( directory );

            long rotateAtSizeBytes = 128;
            int entryCacheSize = 4;
            int metadataCacheSize = 8;
            int fileHeaderCacheSize = 2;

            PhysicalRaftLog log = new PhysicalRaftLog( fileSystem, directory, rotateAtSizeBytes,
                    entryCacheSize, metadataCacheSize, fileHeaderCacheSize,
                    new PhysicalLogFile.Monitor.Adapter(), new DummyRaftableContentSerializer(), () -> mock(
                    DatabaseHealth.class ),
                    NullLogProvider.getInstance() );
            log.start();
            return log;
        };

        return Arrays.asList( new Object[][]{
                {NAIVE, naive},
                {PHYSICAL, physical},
        } );
    }

    @Test
    public void shouldAppendDataAndNotCommitImmediately() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

        final RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntry );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fsRule.get(), myLog -> {
            assertThat( myLog.appendIndex(), is( 0L ) );
            assertThat( myLog.commitIndex(), is( -1L ) );
            assertThat( readLogEntry( myLog, 0 ), equalTo( logEntry ) );
        } );
    }

    @Test
    public void shouldAppendAndCommit() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntry );
        log.commit( 0 );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fsRule.get(), myLog -> {
            assertThat( myLog.appendIndex(), is( 0L ) );
            assertThat( myLog.commitIndex(), is( 0L ) );
        } );
    }

    @Test
    public void shouldAppendAfterReloadingFromFileSystem() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

        RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntryA );

        fsRule.get().crash();
        log = logFactory.createBasedOn( fsRule.get() );

        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );
        log.append( logEntryB );

        assertThat( log.appendIndex(), is( 1L ) );
        assertThat( readLogEntry( log, 0 ), is( logEntryA ) );
        assertThat( readLogEntry( log, 1 ), is( logEntryB ) );
    }

    @Test
    public void shouldTruncatePreviouslyAppendedEntries() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

        RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );

        log.append( logEntryA );
        log.append( logEntryB );
        log.truncate( 1 );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fsRule.get(), myLog -> {
            assertThat( myLog.appendIndex(), is( 0L ) );
        } );
    }

    @Test
    public void shouldReplacePreviouslyAppendedEntries() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

        final RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        final RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );
        final RaftLogEntry logEntryC = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 3 ) );
        final RaftLogEntry logEntryD = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 4 ) );
        final RaftLogEntry logEntryE = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 5 ) );

        log.append( logEntryA );
        log.append( logEntryB );
        log.append( logEntryC );

        log.truncate( 1 );

        log.append( logEntryD );
        log.append( logEntryE );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fsRule.get(), myLog -> {
            assertThat( myLog.appendIndex(), is( 2L ) );
            assertThat( readLogEntry( myLog, 0 ), equalTo( logEntryA ) );
            assertThat( readLogEntry( myLog, 1 ), equalTo( logEntryD ) );
            assertThat( readLogEntry( myLog, 2 ), equalTo( logEntryE ) );
        } );
    }

    @Test
    public void shouldLogDifferentContentTypes() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

        final RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        final RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedString.valueOf( "hejzxcjkzhxcjkxz" ) );

        log.append( logEntryA );
        log.append( logEntryB );

        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fsRule.get(), myLog -> {
            assertThat( myLog.appendIndex(), is( 1L ) );
            assertThat( readLogEntry( myLog, 0 ), equalTo( logEntryA ) );
            assertThat( readLogEntry( myLog, 1 ), equalTo( logEntryB ) );
        } );
    }

    @Test
    public void shouldRecoverAfterEventuallyPruning() throws Exception
    {
        RaftLog log = logFactory.createBasedOn( fsRule.get() );

        long term = 0L;

        long safeIndex;
        long prunedIndex = -1;
        int val = 0;

        // this loop should eventually be able to prune something
        while ( prunedIndex == -1 )
        {
            for ( int i = 0; i < 100; i++ )
            {
                log.append( new RaftLogEntry( term, valueOf( val++ ) ) );
            }
            safeIndex = log.appendIndex() - 50;
            // when
            prunedIndex = log.prune( safeIndex );
        }

        final long finalAppendIndex = log.appendIndex();
        final long finalPrunedIndex = prunedIndex;
        verifyCurrentLogAndNewLogLoadedFromFileSystem( log, fsRule.get(), myLog -> {
            assertThat( myLog.prevIndex(), equalTo( finalPrunedIndex ) );
            assertThat( myLog.appendIndex(), is( finalAppendIndex ) );
            assertThat( readLogEntry( myLog, finalPrunedIndex + 1 ).content(), equalTo( valueOf( (int) finalPrunedIndex + 1 ) ) );
        } );
    }

    private void verifyCurrentLogAndNewLogLoadedFromFileSystem(
            RaftLog log, EphemeralFileSystemAbstraction fileSystem, LogVerifier logVerifier ) throws Exception
    {
        logVerifier.verifyLog( log );
        logVerifier.verifyLog( logFactory.createBasedOn( fsRule.get() ) );
        fileSystem.crash();
        logVerifier.verifyLog( logFactory.createBasedOn( fsRule.get() ) );
    }

    private interface RaftLogFactory
    {
        RaftLog createBasedOn( FileSystemAbstraction fileSystem ) throws Exception;
    }

    private interface LogVerifier
    {
        void verifyLog( RaftLog log ) throws IOException, RaftLogCompactedException;
    }
}
