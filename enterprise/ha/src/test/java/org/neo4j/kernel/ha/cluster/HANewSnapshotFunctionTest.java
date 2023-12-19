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
package org.neo4j.kernel.ha.cluster;

import org.junit.Test;

import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.paxos.MemberIsAvailable;
import org.neo4j.kernel.impl.store.StoreId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.SLAVE;

public class HANewSnapshotFunctionTest
{
    @Test
    public void normalClusterCreationShouldBePassedUnchanged()
    {
        // GIVEN
        // This is what the end result should look like
        List<MemberIsAvailable> events = new LinkedList<>();
        events.add( roleForId( MASTER, 1 ) );
        events.add( roleForId( SLAVE, 2 ) );
        events.add( roleForId( SLAVE, 3 ) );

        // WHEN events start getting added
        Iterable<MemberIsAvailable> result = new LinkedList<>();
        for ( MemberIsAvailable event : events )
        {
            result = new HANewSnapshotFunction().apply( result, event );
        }

        // THEN the result is the expected one
        eventsMatch( result, events );
    }

    @Test
    public void duplicateSlaveEventsShouldBeFilteredOut()
    {
        // GIVEN
        // This is the list of events
        List<MemberIsAvailable> events = new LinkedList<>();
        events.add( roleForId( MASTER, 1 ) );
        events.add( roleForId( SLAVE, 2 ) );
        events.add( roleForId( SLAVE, 3 ) );
        events.add( roleForId( SLAVE, 2 ) );
        events.add( roleForId( SLAVE, 3 ) );
        // This is what it should look like
        List<MemberIsAvailable> expected = new LinkedList<>();
        expected.add( roleForId( MASTER, 1 ) );
        expected.add( roleForId( SLAVE, 2 ) );
        expected.add( roleForId( SLAVE, 3 ) );

        // WHEN events start getting added
        Iterable<MemberIsAvailable> result = new LinkedList<>();
        for ( MemberIsAvailable event : events )
        {
            result = new HANewSnapshotFunction().apply( result, event );
        }

        // THEN the result should be the same as the one above
        eventsMatch( result, expected );
    }

    @Test
    public void instanceBeingMasterReappearsAsSlaveShouldBeTreatedAsSlave()
    {
        // GIVEN these events
        List<MemberIsAvailable> events = new LinkedList<>();
        events.add( roleForId( MASTER, 1 ) );
        events.add( roleForId( SLAVE, 2 ) );
        events.add( roleForId( SLAVE, 1 ) );
        events.add( roleForId( SLAVE, 3 ) );
        // and this expected outcome
        List<MemberIsAvailable> expected = new LinkedList<>();
        expected.add( roleForId( SLAVE, 2 ) );
        expected.add( roleForId( SLAVE, 1 ) );
        expected.add( roleForId( SLAVE, 3 ) );

        // WHEN events start getting added
        Iterable<MemberIsAvailable> result = new LinkedList<>();
        for ( MemberIsAvailable event : events )
        {
            result = new HANewSnapshotFunction().apply( result, event );
        }

        // THEN the result should be the expected one
        eventsMatch( result, expected );
    }

    @Test
    public void instanceBeingSlaveReappearsAsMasterShouldBeTreatedAsMaster()
    {
        // GIVEN these events
        List<MemberIsAvailable> events = new LinkedList<>();
        events.add( roleForId( SLAVE, 2 ) );
        events.add( roleForId( SLAVE, 1 ) );
        events.add( roleForId( MASTER, 1 ) );
        events.add( roleForId( SLAVE, 3 ) );
        // and this expected outcome
        List<MemberIsAvailable> expected = new LinkedList<>();
        expected.add( roleForId( SLAVE, 2 ) );
        expected.add( roleForId( MASTER, 1 ) );
        expected.add( roleForId( SLAVE, 3 ) );

        // WHEN events start getting added
        Iterable<MemberIsAvailable> result = new LinkedList<>();
        for ( MemberIsAvailable event : events )
        {
            result = new HANewSnapshotFunction().apply( result, event );
        }

        // THEN the result should be the expected one
        eventsMatch( result, expected );
    }

    @Test
    public void instanceBeingMasterReplacedByAnotherInstanceShouldNotRemainMaster()
    {
        // GIVEN these events
        List<MemberIsAvailable> events = new LinkedList<>();
        events.add( roleForId( MASTER, 1 ) );
        events.add( roleForId( MASTER, 2 ) );
        events.add( roleForId( SLAVE, 3 ) );
        // and this expected outcome
        List<MemberIsAvailable> expected = new LinkedList<>();
        expected.add( roleForId( MASTER, 2 ) );
        expected.add( roleForId( SLAVE, 3 ) );

        // WHEN events start getting added
        Iterable<MemberIsAvailable> result = new LinkedList<>();
        for ( MemberIsAvailable event : events )
        {
            result = new HANewSnapshotFunction().apply( result, event );
        }

        // THEN the result should be the expected one
        eventsMatch( result, expected );
    }

    @Test
    public void instanceBeingBackupReplacedByAnotherInstanceShouldNotRemainBackup()
    {
        // GIVEN these events
        List<MemberIsAvailable> events = new LinkedList<>();
        events.add( roleForId( OnlineBackupKernelExtension.BACKUP, 1 ) );
        events.add( roleForId( MASTER, 2 ) );
        events.add( roleForId( OnlineBackupKernelExtension.BACKUP, 2 ) );
        events.add( roleForId( SLAVE, 3 ) );
        // and this expected outcome
        List<MemberIsAvailable> expected = new LinkedList<>();
        expected.add( roleForId( MASTER, 2 ) );
        expected.add( roleForId( OnlineBackupKernelExtension.BACKUP, 2 ) );
        expected.add( roleForId( SLAVE, 3 ) );

        // WHEN events start getting added
        Iterable<MemberIsAvailable> result = new LinkedList<>();
        for ( MemberIsAvailable event : events )
        {
            result = new HANewSnapshotFunction().apply( result, event );
        }

        // THEN the result should be the expected one
        eventsMatch( result, expected );
    }

    @Test
    public void instanceBeingBackupRepeatedlyShouldRemainBackupOnceOnly()
    {
        // GIVEN these events
        List<MemberIsAvailable> events = new LinkedList<>();
        events.add( roleForId( OnlineBackupKernelExtension.BACKUP, 1 ) );
        events.add( roleForId( OnlineBackupKernelExtension.BACKUP, 1 ) );
        events.add( roleForId( OnlineBackupKernelExtension.BACKUP, 1 ) );
        events.add( roleForId( OnlineBackupKernelExtension.BACKUP, 1 ) );
        // and this expected outcome
        List<MemberIsAvailable> expected = new LinkedList<>();
        expected.add( roleForId( OnlineBackupKernelExtension.BACKUP, 1 ) );

        // WHEN events start getting added
        Iterable<MemberIsAvailable> result = new LinkedList<>();
        for ( MemberIsAvailable event : events )
        {
            result = new HANewSnapshotFunction().apply( result, event );
        }

        // THEN the result should be the expected one
        eventsMatch( result, expected );
    }

    private MemberIsAvailable roleForId( String role, int id )
    {
        return new MemberIsAvailable( role, new InstanceId( id ), URI.create( "cluster://" + id ),
                URI.create( "ha://" + id ), StoreId.DEFAULT );
    }

    private void eventsMatch( Iterable<MemberIsAvailable> result, List<MemberIsAvailable> expected )
    {
        Iterator<MemberIsAvailable> iter = result.iterator();
        for ( MemberIsAvailable anExpected : expected )
        {
            assertEquals( anExpected, iter.next() );
        }
        assertFalse( iter.hasNext() );
    }
}
