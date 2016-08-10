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
package org.neo4j.coreedge.core.state.storage;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.test.rule.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class DurableStateStorageTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public LifeRule lifeRule = new LifeRule( true );

    @Test
    public void shouldMaintainStateGivenAnEmptyInitialStore() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        DurableStateStorage<AtomicInteger> storage = lifeRule.add( new DurableStateStorage<>( fsa, testDir.directory(),
                "state", new AtomicIntegerMarshal(), 100, health(), NullLogProvider.getInstance(), false ) );

        // when
        storage.persistStoreData( new AtomicInteger( 99 ) );

        // then
        assertEquals( 4, fsa.getFileSize( stateFileA() ) );
    }

    @Test
    public void shouldRotateToOtherStoreFileAfterSufficientEntries() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        final int numberOfEntriesBeforeRotation = 100;
        DurableStateStorage<AtomicInteger> storage = lifeRule.add( new DurableStateStorage<>( fsa, testDir.directory(),
                "state", new AtomicIntegerMarshal(), numberOfEntriesBeforeRotation,
                health(), NullLogProvider.getInstance(), false ) );

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
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        final int numberOfEntriesBeforeRotation = 100;
        DurableStateStorage<AtomicInteger> storage = lifeRule.add( new DurableStateStorage<>( fsa, testDir.directory(),
                "state", new AtomicIntegerMarshal(), numberOfEntriesBeforeRotation,
                health(), NullLogProvider.getInstance(), false ) );

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
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );

        int rotationCount = 10;

        DurableStateStorage<AtomicInteger> storage = new DurableStateStorage<>( fsa, testDir.directory(),
                "state", new AtomicIntegerMarshal(), rotationCount,
                health(), NullLogProvider.getInstance(), false );
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
                "state", new AtomicIntegerMarshal(), rotationCount,
                health(), NullLogProvider.getInstance(), false ) );

        storage.persistStoreData( new AtomicInteger( largestValueWritten++ ) );
        storage.persistStoreData( new AtomicInteger( largestValueWritten++ ) );
        storage.persistStoreData( new AtomicInteger( largestValueWritten ) );

        /*
         * We have written stuff in fileA but not gotten to the end (resulting in rotation). The largestValueWritten
         * should nevertheless be correct
         */
        ByteBuffer forReadingBackIn = ByteBuffer.allocate( 10_000 );
        StoreChannel lastWrittenTo = fsa.open( stateFileA(), "r" );
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

    @SuppressWarnings("unchecked")
    private Supplier<DatabaseHealth> health()
    {
        return mock( Supplier.class );
    }
}
