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
package org.neo4j.coreedge.raft.state;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.MethodGuardedAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.SelectiveFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.internal.KernelEventHandlers;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class DurableStateStorageIT
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldRecoverAfterCrashUnderLoad() throws Exception
    {
        EphemeralFileSystemAbstraction delegate = new EphemeralFileSystemAbstraction();
        AdversarialFileSystemAbstraction fsa = new AdversarialFileSystemAbstraction(
                new MethodGuardedAdversary( new CountingAdversary( 100, true ),
                        StoreChannel.class.getMethod( "writeAll", ByteBuffer.class ) ),
                delegate );

        LongState persistedState = new LongState( fsa, testDir.directory(), 14 );
        long lastValue = 0;

        try
        {
            while ( true ) // it will break from the Exception that AFS will throw
            {
                long tempValue = lastValue + 1;
                persistedState.setTheState( tempValue );
                lastValue = tempValue;
            }
        }
        catch ( Exception expected )
        {
            ensureStackTraceContainsExpectedMethod( expected.getStackTrace(), "writeAll" );
        }

        LongState restoredState = new LongState( delegate, testDir.directory(), 4 );
        assertEquals( lastValue, restoredState.getTheState() );
    }

    @Test
    public void shouldProperlyRecoveryAfterCrashOnFileCreationDuringRotation() throws Exception
    {
        EphemeralFileSystemAbstraction normalFSA = new EphemeralFileSystemAbstraction();
        /*
         * Magic number warning. For a rotation threshold of 14, 998 operations on file A falls on truncation of the
         * file during rotation. This has been discovered via experimentation. The end result is that there is a
         * failure to create the file to rotate to. This should be recoverable.
         */
        AdversarialFileSystemAbstraction breakingFSA = new AdversarialFileSystemAbstraction(
                new MethodGuardedAdversary(
                        new CountingAdversary( 20, true ),
                        FileSystemAbstraction.class.getMethod( "truncate", File.class, long.class ) ),
                normalFSA );
        SelectiveFileSystemAbstraction combinedFSA = new SelectiveFileSystemAbstraction(
                new File( testDir.directory(), "long.a" ), breakingFSA, normalFSA );

        LongState persistedState = new LongState( combinedFSA, testDir.directory(), 14 );
        long lastValue = 0;

        try
        {
            while ( true ) // it will break from the Exception that AFS will throw
            {
                long tempValue = lastValue + 1;
                persistedState.setTheState( tempValue );
                lastValue = tempValue;
            }
        }
        catch ( Exception expected )
        {
            // this stack trace should contain FSA.truncate()
            ensureStackTraceContainsExpectedMethod( expected.getStackTrace(), "truncate" );
        }

        LongState restoredState = new LongState( normalFSA, testDir.directory(), 14 );
        assertEquals( lastValue, restoredState.getTheState() );
    }

    @Test
    public void shouldProperlyRecoveryAfterCrashOnFileForceDuringWrite() throws Exception
    {
        EphemeralFileSystemAbstraction normalFSA = new EphemeralFileSystemAbstraction();
        /*
         * Magic number warning. For a rotation threshold of 14, 990 operations on file A falls on a force() of the
         * current active file. This has been discovered via experimentation. The end result is that there is a
         * flush (but not write) a value. This should be recoverable. Interestingly, the failure semantics are a bit
         * unclear on what should happen to that value. We assume that exception during persistence requires recovery
         * to discover if the last argument made it to disk or not. Since we use an EFSA, force is not necessary and
         * the value that caused the failure is actually "persisted" and recovered.
         */
        AdversarialFileSystemAbstraction breakingFSA = new AdversarialFileSystemAbstraction(
                new MethodGuardedAdversary(
                        new CountingAdversary( 40, true ),
                        StoreChannel.class.getMethod( "force", boolean.class ) ),
                normalFSA );
        SelectiveFileSystemAbstraction combinedFSA = new SelectiveFileSystemAbstraction(
                new File( testDir.directory(), "long.a" ), breakingFSA, normalFSA );

        LongState persistedState = new LongState( combinedFSA, testDir.directory(), 14 );
        long lastValue = 0;

        try
        {
            while ( true ) // it will break from the Exception that AFS will throw
            {
                long tempValue = lastValue + 1;
                persistedState.setTheState( tempValue );
                lastValue = tempValue;
            }
        }
        catch ( Exception expected )
        {
            // this stack trace should contain force()
            ensureStackTraceContainsExpectedMethod( expected.getStackTrace(), "force" );
        }

        LongState restoredState = new LongState( normalFSA, testDir.directory(), 14 );
        assertThat( restoredState.getTheState(), greaterThanOrEqualTo( lastValue ) );
    }

    @Test
    public void shouldProperlyRecoveryAfterCrashingDuringRecovery() throws Exception
    {
        EphemeralFileSystemAbstraction normalFSA = new EphemeralFileSystemAbstraction();
        LongState persistedState = new LongState( normalFSA, testDir.directory(), 14 );

        long lastValue = 0;

        for ( int i = 0; i < 100; i++ )
        {
            long tempValue = lastValue + 1;
            persistedState.setTheState( tempValue );
            lastValue = tempValue;
        }

        try
        {
            // We create a new state that will attempt recovery. The AFS will make it fail on open() of one of the files
            new LongState( new AdversarialFileSystemAbstraction(
                    new MethodGuardedAdversary(
                            new CountingAdversary( 1, true ),
                            FileSystemAbstraction.class.getMethod( "open", File.class, String.class ) ),
                    normalFSA ), testDir.directory(), 14 );
            fail( "Should have failed recovery" );
        }
        catch ( Exception expected )
        {
            // this stack trace should contain open()
            ensureStackTraceContainsExpectedMethod( expected.getStackTrace(), "open" );
        }

        // Recovery over the normal filesystem after a failed recovery should proceed correctly
        LongState recoveredState = new LongState( normalFSA, testDir.directory(), 14 );
        assertThat( recoveredState.getTheState(), greaterThanOrEqualTo( lastValue ) );
    }

    @Test
    public void shouldProperlyRecoveryAfterCloseOnActiveFileDuringRotation() throws Exception
    {
        EphemeralFileSystemAbstraction normalFSA = new EphemeralFileSystemAbstraction();
        AdversarialFileSystemAbstraction breakingFSA = new AdversarialFileSystemAbstraction(
                new MethodGuardedAdversary(
                        new CountingAdversary( 5, true ),
                        StoreChannel.class.getMethod( "close" ) ),
                normalFSA );
        SelectiveFileSystemAbstraction combinedFSA = new SelectiveFileSystemAbstraction(
                new File( testDir.directory(), "long.a" ), breakingFSA, normalFSA );

        LongState persistedState = new LongState( combinedFSA, testDir.directory(), 14 );
        long lastValue = 0;

        try
        {
            while ( true ) // it will break from the Exception that AFS will throw
            {
                long tempValue = lastValue + 1;
                persistedState.setTheState( tempValue );
                lastValue = tempValue;
            }
        }
        catch ( Exception expected )
        {
            // this stack trace should contain close()
            ensureStackTraceContainsExpectedMethod( expected.getStackTrace(), "close" );
        }

        LongState restoredState = new LongState( normalFSA, testDir.directory(), 14 );
        assertThat( restoredState.getTheState(), greaterThanOrEqualTo( lastValue ) );
    }

    private void ensureStackTraceContainsExpectedMethod( StackTraceElement[] stackTrace, String expectedMethodName )
    {
        for ( StackTraceElement stackTraceElement : stackTrace )
        {
            if ( stackTraceElement.getMethodName().equals( expectedMethodName ) )
            {
                return;
            }
        }
        fail( "Method " + expectedMethodName + " was not part of the failure stack trace." );
    }

    private static class LongState
    {
        private static final String FILENAME = "long";
        private final DurableStateStorage<Long> stateStorage;
        private long theState = -1;

        public LongState( FileSystemAbstraction fileSystemAbstraction, File stateDir,
                          int numberOfEntriesBeforeRotation ) throws IOException
        {
            StateMarshal<Long> byteBufferMarshal = new StateMarshal<Long>()
            {
                @Override
                public Long startState()
                {
                    return 0L;
                }

                @Override
                public long ordinal( Long aLong )
                {
                    return aLong;
                }

                @Override
                public void marshal( Long aLong, WritableChannel channel ) throws IOException
                {
                    channel.putLong( aLong );
                }

                @Override
                public Long unmarshal( ReadableChannel source ) throws IOException
                {
                    try
                    {
                        return source.getLong();
                    }
                    catch ( ReadPastEndException notEnoughBytes )
                    {
                        return null;
                    }
                }
            };

            Supplier<DatabaseHealth> databaseHealthSupplier = () -> new DatabaseHealth( new
                    DatabasePanicEventGenerator( new KernelEventHandlers( NullLog
                    .getInstance() ) ), NullLog.getInstance() );
            this.stateStorage = new DurableStateStorage<>( fileSystemAbstraction, stateDir, FILENAME,
                    byteBufferMarshal,
                    numberOfEntriesBeforeRotation,
                    databaseHealthSupplier,
                    NullLogProvider.getInstance()
            );

            this.theState = this.stateStorage.getInitialState();
        }

        public long getTheState()
        {
            return theState;
        }

        public void setTheState( long newState ) throws IOException
        {
            stateStorage.persistStoreData( newState );
            this.theState = newState;
        }
    }
}
