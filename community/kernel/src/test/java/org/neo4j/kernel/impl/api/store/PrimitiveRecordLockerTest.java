/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrimitiveRecordLockerTest
{
    @Test
    public void acquireNodeLock()
    {
        LockService lockService = new ReentrantLockService();
        NodeRecord record = newNodeRecord( 42 );
        NodeStore store = mock( NodeStore.class );
        when( store.loadRecord( record.getId(), record ) ).thenReturn( record );

        try ( Lock lock = PrimitiveRecordLocker.shortLivedReadLock( lockService, record, store ) )
        {
            assertNotNull( lock );
        }
    }

    @Test
    public void acquireNodeLockWithDummyLockService()
    {
        NodeStore store = mock( NodeStore.class );
        NodeRecord record = newNodeRecord( 42 );

        Lock lock = PrimitiveRecordLocker.shortLivedReadLock( LockService.NO_LOCK_SERVICE, record, store );

        assertSame( LockService.NO_LOCK, lock );
    }

    @Test
    public void acquireNodeAfterNodeRecordWasRemoved()
    {
        LockService lockService = new ReentrantLockService();
        NodeRecord record = newNodeRecord( 42 );
        NodeStore store = mock( NodeStore.class );
        when( store.loadRecord( record.getId(), record ) ).thenAnswer( new Answer<NodeRecord>()
        {
            @Override
            public NodeRecord answer( InvocationOnMock invocation ) throws Throwable
            {
                NodeRecord nodeRecord = (NodeRecord) invocation.getArguments()[1];
                nodeRecord.setInUse( false );
                nodeRecord.setNextProp( 4242 );
                return nodeRecord;
            }
        } );

        try ( Lock lock = PrimitiveRecordLocker.shortLivedReadLock( lockService, record, store ) )
        {
            assertNotNull( lock );
            assertEquals( Record.NO_NEXT_PROPERTY.intValue(), record.getNextProp() );
        }
    }

    @Test
    public void nodeLockReleasedWhenReReadFails()
    {
        LockService lockService = mock( LockService.class );
        Lock nodeLock = mock( Lock.class );
        when( lockService.acquireNodeLock( 42, LockService.LockType.READ_LOCK ) ).thenReturn( nodeLock );
        NodeRecord record = newNodeRecord( 42 );
        NodeStore store = mock( NodeStore.class );
        Exception failure = new RuntimeException( "Re-read failed" );
        when( store.loadRecord( record.getId(), record ) ).thenThrow( failure );

        try
        {
            PrimitiveRecordLocker.shortLivedReadLock( lockService, record, store );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertSame( failure, e );
            verify( lockService ).acquireNodeLock( 42, LockService.LockType.READ_LOCK );
            verify( nodeLock ).release();
        }
    }

    @Test
    public void acquireRelationshipLock()
    {
        LockService lockService = new ReentrantLockService();
        RelationshipRecord record = newRelationshipRecord( 42 );
        RelationshipStore store = mock( RelationshipStore.class );
        when( store.fillRecord( record.getId(), record, RecordLoad.FORCE ) ).thenReturn( true );

        try ( Lock lock = PrimitiveRecordLocker.shortLivedReadLock( lockService, record, store ) )
        {
            assertNotNull( lock );
        }
    }

    @Test
    public void acquireRelationshipLockWithDummyLockService()
    {
        RelationshipStore store = mock( RelationshipStore.class );
        RelationshipRecord record = newRelationshipRecord( 42 );

        try ( Lock lock = PrimitiveRecordLocker.shortLivedReadLock( LockService.NO_LOCK_SERVICE, record, store ) )
        {
            assertSame( LockService.NO_LOCK, lock );
        }
    }

    @Test
    public void acquireRelationshipAfterRelationshipRecordWasRemoved()
    {
        LockService lockService = new ReentrantLockService();
        RelationshipRecord record = newRelationshipRecord( 42 );
        RelationshipStore store = mock( RelationshipStore.class );
        when( store.fillRecord( record.getId(), record, RecordLoad.FORCE ) ).thenAnswer(
                new Answer<Boolean>()
                {
                    @Override
                    public Boolean answer( InvocationOnMock invocation ) throws Throwable
                    {
                        RelationshipRecord relRecord = (RelationshipRecord) invocation.getArguments()[1];
                        relRecord.setInUse( false );
                        relRecord.setNextProp( 4242 );
                        return false;
                    }
                } );

        try ( Lock lock = PrimitiveRecordLocker.shortLivedReadLock( lockService, record, store ) )
        {
            assertNotNull( lock );
            assertEquals( Record.NO_NEXT_PROPERTY.intValue(), record.getNextProp() );
        }
    }

    @Test
    public void relationshipLockReleasedWhenReReadFails()
    {
        LockService lockService = mock( LockService.class );
        Lock relLock = mock( Lock.class );
        when( lockService.acquireRelationshipLock( 42, LockService.LockType.READ_LOCK ) ).thenReturn( relLock );
        RelationshipRecord record = newRelationshipRecord( 42 );
        RelationshipStore store = mock( RelationshipStore.class );
        Exception failure = new RuntimeException( "Re-read failed" );
        when( store.fillRecord( record.getId(), record, RecordLoad.FORCE ) ).thenThrow( failure );

        try
        {
            PrimitiveRecordLocker.shortLivedReadLock( lockService, record, store );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertSame( failure, e );
            verify( lockService ).acquireRelationshipLock( 42, LockService.LockType.READ_LOCK );
            verify( relLock ).release();
        }
    }

    private static NodeRecord newNodeRecord( long id )
    {
        NodeRecord record = new NodeRecord( id );
        record.setInUse( true );
        return record;
    }

    private static RelationshipRecord newRelationshipRecord( long id )
    {
        RelationshipRecord record = new RelationshipRecord( id );
        record.setInUse( true );
        return record;
    }
}
