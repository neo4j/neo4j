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
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.raft.state.membership.Marshal;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import static org.neo4j.coreedge.raft.state.id_allocation.OnDiskIdAllocationState.FILENAME;

public class StatePersisterTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldMaintainStateGivenAnEmptyInitialStore() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
        fsa.mkdir( testDir.directory() );


        StatePersister<AtomicInteger> statePersister = new StatePersister<>( stateFileA(), stateFileB(), fsa, 100,
                ByteBuffer.allocate( 10_000 ), new AtomicIntegerMarshal(), stateFileA(),
                mock( Supplier.class ) );

        // when
        statePersister.persistStoreData( new AtomicInteger( 99 ) );

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
        StatePersister<AtomicInteger> statePersister = new StatePersister<>( stateFileA(), stateFileB(), fsa,
                numberOfEntriesBeforeRotation,
                ByteBuffer.allocate( 10_000 ), new AtomicIntegerMarshal(), stateFileA(),
                mock( Supplier.class ) );


        // when
        for ( int i = 0; i < numberOfEntriesBeforeRotation; i++ )
        {
            statePersister.persistStoreData( new AtomicInteger( i ) );
        }

        // Force the rotation
        statePersister.persistStoreData( new AtomicInteger( 9999 ) );

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
        StatePersister<AtomicInteger> statePersister = new StatePersister<>( stateFileA(), stateFileB(), fsa,
                numberOfEntriesBeforeRotation,
                ByteBuffer.allocate( 10_000 ), new AtomicIntegerMarshal(), stateFileA(),
                mock( Supplier.class ) );


        // when
        for ( int i = 0; i < numberOfEntriesBeforeRotation * 2; i++ )
        {
            statePersister.persistStoreData( new AtomicInteger( i ) );
        }

        // Force the rotation back to the first store
        statePersister.persistStoreData( new AtomicInteger( 9999 ) );

        // then
        assertEquals( 4, fsa.getFileSize( stateFileA() ) );
        assertEquals( numberOfEntriesBeforeRotation * 4, fsa.getFileSize( stateFileB() ) );
    }

    private static class AtomicIntegerMarshal implements Marshal<AtomicInteger>
    {
        @Override
        public void marshal( AtomicInteger atomicInteger, ByteBuffer buffer )
        {
            buffer.putInt( atomicInteger.intValue() );
        }

        @Override
        public AtomicInteger unmarshal( ByteBuffer source )
        {
            return new AtomicInteger( source.getInt() );
        }
    }

    private File stateFileA()
    {
        return new File( testDir.directory(), FILENAME + "A" );
    }

    private File stateFileB()
    {
        return new File( testDir.directory(), FILENAME + "B" );
    }
}