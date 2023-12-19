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
package org.neo4j.causalclustering.core.state.storage;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DurableStateStorageTest
{
    private final TestDirectory testDir = TestDirectory.testDirectory();
    private final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    private final LifeRule lifeRule = new LifeRule( true );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( lifeRule ).around( testDir );

    @Test
    public void shouldMaintainStateGivenAnEmptyInitialStore() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        DurableStateStorage<AtomicInteger> storage = lifeRule.add( new DurableStateStorage<>( fsa, testDir.directory(),
                "state", new AtomicIntegerMarshal(), 100, NullLogProvider.getInstance() ) );

        // when
        storage.persistStoreData( new AtomicInteger( 99 ) );

        // then
        assertEquals( 4, fsa.getFileSize( stateFileA() ) );
    }

    @Test
    public void shouldRotateToOtherStoreFileAfterSufficientEntries() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        final int numberOfEntriesBeforeRotation = 100;
        DurableStateStorage<AtomicInteger> storage = lifeRule.add( new DurableStateStorage<>( fsa, testDir.directory(),
                "state", new AtomicIntegerMarshal(), numberOfEntriesBeforeRotation, NullLogProvider.getInstance() ) );

        // when
        for ( int i = 0; i < numberOfEntriesBeforeRotation; i++ )
        {
            storage.persistStoreData( new AtomicInteger( i ) );
        }

        // Force the rotation
        storage.persistStoreData( new AtomicInteger( 9999 ) );

        // then
        assertEquals( 4, fsa.getFileSize( stateFileB() ) );
        assertEquals( numberOfEntriesBeforeRotation * 4, fsa.getFileSize( stateFileA() ) );
    }

    @Test
    public void shouldRotateBackToFirstStoreFileAfterSufficientEntries() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        final int numberOfEntriesBeforeRotation = 100;
        DurableStateStorage<AtomicInteger> storage = lifeRule.add( new DurableStateStorage<>( fsa, testDir.directory(),
                "state", new AtomicIntegerMarshal(), numberOfEntriesBeforeRotation, NullLogProvider.getInstance() ) );

        // when
        for ( int i = 0; i < numberOfEntriesBeforeRotation * 2; i++ )
        {
            storage.persistStoreData( new AtomicInteger( i ) );
        }

        // Force the rotation back to the first store
        storage.persistStoreData( new AtomicInteger( 9999 ) );

        // then
        assertEquals( 4, fsa.getFileSize( stateFileA() ) );
        assertEquals( numberOfEntriesBeforeRotation * 4, fsa.getFileSize( stateFileB() ) );
    }

    @Test
    public void shouldClearFileOnFirstUse() throws Throwable
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        int rotationCount = 10;

        DurableStateStorage<AtomicInteger> storage = new DurableStateStorage<>( fsa, testDir.directory(),
                "state", new AtomicIntegerMarshal(), rotationCount, NullLogProvider.getInstance() );
        int largestValueWritten = 0;
        try ( Lifespan lifespan = new Lifespan( storage ) )
        {
            for ( ; largestValueWritten < rotationCount * 2; largestValueWritten++ )
            {
                storage.persistStoreData( new AtomicInteger( largestValueWritten ) );
            }
        }

        // now both files are full. We reopen, then write some more.
        storage = lifeRule.add( new DurableStateStorage<>( fsa, testDir.directory(),
                "state", new AtomicIntegerMarshal(), rotationCount, NullLogProvider.getInstance() ) );

        storage.persistStoreData( new AtomicInteger( largestValueWritten++ ) );
        storage.persistStoreData( new AtomicInteger( largestValueWritten++ ) );
        storage.persistStoreData( new AtomicInteger( largestValueWritten ) );

        /*
         * We have written stuff in fileA but not gotten to the end (resulting in rotation). The largestValueWritten
         * should nevertheless be correct
         */
        ByteBuffer forReadingBackIn = ByteBuffer.allocate( 10_000 );
        StoreChannel lastWrittenTo = fsa.open( stateFileA(), OpenMode.READ );
        lastWrittenTo.read( forReadingBackIn );
        forReadingBackIn.flip();

        AtomicInteger lastRead = null;
        while ( true )
        {
            try
            {
                lastRead = new AtomicInteger( forReadingBackIn.getInt() );
            }
            catch ( BufferUnderflowException e )
            {
                break;
            }
        }

        // then
        assertNotNull( lastRead );
        assertEquals( largestValueWritten, lastRead.get() );
    }

    private static class AtomicIntegerMarshal extends SafeStateMarshal<AtomicInteger>
    {
        @Override
        public void marshal( AtomicInteger state, WritableChannel channel ) throws IOException
        {
            channel.putInt( state.intValue() );
        }

        @Override
        public AtomicInteger unmarshal0( ReadableChannel channel ) throws IOException
        {
            return new AtomicInteger( channel.getInt() );
        }

        @Override
        public AtomicInteger startState()
        {
            return new AtomicInteger( 0 );
        }

        @Override
        public long ordinal( AtomicInteger atomicInteger )
        {
            return atomicInteger.get();
        }
    }

    private File stateFileA()
    {
        return new File( new File( testDir.directory(), "state-state" ), "state.a" );
    }

    private File stateFileB()
    {
        return new File( new File( testDir.directory(), "state-state" ), "state.b" );
    }
}
