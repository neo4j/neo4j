/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.storage;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.MethodGuardedAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.SelectiveFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class DurableStateStorageIT
{

    private final TestDirectory testDir = TestDirectory.testDirectory( getClass() );
    private final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( testDir );

    @Test
    public void shouldRecoverAfterCrashUnderLoad() throws Exception
    {
        EphemeralFileSystemAbstraction delegate = fileSystemRule.get();
        AdversarialFileSystemAbstraction fsa = new AdversarialFileSystemAbstraction(
                new MethodGuardedAdversary( new CountingAdversary( 100, true ),
                        StoreChannel.class.getMethod( "writeAll", ByteBuffer.class ) ),
                delegate );

        long lastValue = 0;
        try ( LongState persistedState = new LongState( fsa, testDir.directory(), 14 ) )
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

        try ( LongState restoredState = new LongState( delegate, testDir.directory(), 4 ) )
        {
            assertEquals( lastValue, restoredState.getTheState() );
        }
    }

    @Test
    public void shouldProperlyRecoveryAfterCrashOnFileCreationDuringRotation() throws Exception
    {
        EphemeralFileSystemAbstraction normalFSA = fileSystemRule.get();
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
                new File( new File( testDir.directory(), "long-state" ), "long.a" ), breakingFSA, normalFSA );

        long lastValue = 0;
        try ( LongState persistedState = new LongState( combinedFSA, testDir.directory(), 14 ) )
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

        try ( LongState restoredState = new LongState( normalFSA, testDir.directory(), 14 ) )
        {
            assertEquals( lastValue, restoredState.getTheState() );
        }
    }

    @Test
    public void shouldProperlyRecoveryAfterCrashOnFileForceDuringWrite() throws Exception
    {
        EphemeralFileSystemAbstraction normalFSA = fileSystemRule.get();
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
                new File( new File( testDir.directory(), "long-state" ), "long.a" ), breakingFSA, normalFSA );

        long lastValue = 0;

        try ( LongState persistedState = new LongState( combinedFSA, testDir.directory(), 14 ) )
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

        try ( LongState restoredState = new LongState( normalFSA, testDir.directory(), 14 ) )
        {
            assertThat( restoredState.getTheState(), greaterThanOrEqualTo( lastValue ) );
        }
    }

    @Test
    public void shouldProperlyRecoveryAfterCrashingDuringRecovery() throws Exception
    {
        EphemeralFileSystemAbstraction normalFSA = fileSystemRule.get();

        long lastValue = 0;

        try ( LongState persistedState = new LongState( normalFSA, testDir.directory(), 14 ) )
        {
            for ( int i = 0; i < 100; i++ )
            {
                long tempValue = lastValue + 1;
                persistedState.setTheState( tempValue );
                lastValue = tempValue;
            }
        }

        try
        {
            // We create a new state that will attempt recovery. The AFS will make it fail on open() of one of the files
            new LongState( new AdversarialFileSystemAbstraction(
                    new MethodGuardedAdversary(
                            new CountingAdversary( 1, true ),
                            FileSystemAbstraction.class.getMethod( "open", File.class, OpenMode.class ) ),
                    normalFSA ), testDir.directory(), 14 );
            fail( "Should have failed recovery" );
        }
        catch ( Exception expected )
        {
            // this stack trace should contain open()
            ensureStackTraceContainsExpectedMethod( expected.getCause().getStackTrace(), "open" );
        }

        // Recovery over the normal filesystem after a failed recovery should proceed correctly
        try ( LongState recoveredState = new LongState( normalFSA, testDir.directory(), 14 ) )
        {
            assertThat( recoveredState.getTheState(), greaterThanOrEqualTo( lastValue ) );
        }
    }

    @Test
    public void shouldProperlyRecoveryAfterCloseOnActiveFileDuringRotation() throws Exception
    {
        EphemeralFileSystemAbstraction normalFSA = fileSystemRule.get();
        AdversarialFileSystemAbstraction breakingFSA = new AdversarialFileSystemAbstraction(
                new MethodGuardedAdversary(
                        new CountingAdversary( 5, true ),
                        StoreChannel.class.getMethod( "close" ) ),
                normalFSA );
        SelectiveFileSystemAbstraction combinedFSA = new SelectiveFileSystemAbstraction(
                new File( new File( testDir.directory(), "long-state" ), "long.a" ), breakingFSA, normalFSA );

        long lastValue = 0;
        try ( LongState persistedState = new LongState( combinedFSA, testDir.directory(), 14 ) )
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

        try ( LongState restoredState = new LongState( normalFSA, testDir.directory(), 14 ) )
        {
            assertThat( restoredState.getTheState(), greaterThanOrEqualTo( lastValue ) );
        }
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

    private static class LongState implements AutoCloseable
    {
        private static final String FILENAME = "long";
        private final DurableStateStorage<Long> stateStorage;
        private long theState = -1;
        private LifeSupport lifeSupport = new LifeSupport();

        LongState( FileSystemAbstraction fileSystemAbstraction, File stateDir,
                   int numberOfEntriesBeforeRotation )
        {
            lifeSupport.start();

            StateMarshal<Long> byteBufferMarshal = new SafeStateMarshal<Long>()
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
                public Long unmarshal0( ReadableChannel channel ) throws IOException
                {
                    return channel.getLong();
                }
            };

            this.stateStorage = lifeSupport.add( new DurableStateStorage<>( fileSystemAbstraction, stateDir, FILENAME,
                    byteBufferMarshal,
                    numberOfEntriesBeforeRotation, NullLogProvider.getInstance()
            ) );

            this.theState = this.stateStorage.getInitialState();
        }

        long getTheState()
        {
            return theState;
        }

        void setTheState( long newState ) throws IOException
        {
            stateStorage.persistStoreData( newState );
            this.theState = newState;
        }

        @Override
        public void close()
        {
            lifeSupport.shutdown();
        }
    }
}
