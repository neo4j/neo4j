/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.backup.OnlineBackupKernelExtension.BACKUP;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.SLAVE;

import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.paxos.MemberIsAvailable;

public class HANewSnapshotFunctionTest
{
    @Test
    public void normalClusterCreationShouldBePassedUnchanged() throws Exception
    {
        // GIVEN
        // This is what the end result should look like
        List<MemberIsAvailable> events = new LinkedList<MemberIsAvailable>();
        events.add( roleForId( MASTER, 1 ) );
        events.add( roleForId( BACKUP, 1 ) );
        events.add( roleForId( SLAVE, 2 ) );
        events.add( roleForId( SLAVE, 3 ) );

        // WHEN events start getting added
        Iterable<MemberIsAvailable > result = new LinkedList<MemberIsAvailable>();
        for ( MemberIsAvailable event : events )
        {
            result = new HANewSnapshotFunction().apply( result, event );
        }

        // THEN the result is the expected one
        eventsMatch( result, events );
    }

    @Test
    public void duplicateSlaveEventsShouldBeFilteredOut() throws Exception
    {
        // GIVEN
        // This is the list of events
        List<MemberIsAvailable> events = new LinkedList<MemberIsAvailable>();
        events.add( roleForId( MASTER, 1 ) );
        events.add( roleForId( BACKUP, 1 ) );
        events.add( roleForId( SLAVE, 2 ) );
        events.add( roleForId( SLAVE, 3 ) );
        events.add( roleForId( SLAVE, 2 ) );
        events.add( roleForId( SLAVE, 3 ) );
        // This is what it should look like
        List<MemberIsAvailable> expected = new LinkedList<MemberIsAvailable>();
        expected.add( roleForId( MASTER, 1 ) );
        expected.add( roleForId( BACKUP, 1 ) );
        expected.add( roleForId( SLAVE, 2 ) );
        expected.add( roleForId( SLAVE, 3 ) );

        // WHEN events start getting added
        Iterable<MemberIsAvailable > result = new LinkedList<MemberIsAvailable>();
        for ( MemberIsAvailable event : events )
        {
            result = new HANewSnapshotFunction().apply( result, event );
        }

        // THEN the result should be the same as the one above
        eventsMatch( result, expected );
    }

    @Test
    public void instanceBeingMasterReappearsAsSlaveShouldBeTreatedAsSlave() throws Exception
    {
        // GIVEN these events
        List<MemberIsAvailable> events = new LinkedList<MemberIsAvailable>();
        events.add( roleForId( MASTER, 1 ) );
        events.add( roleForId( BACKUP, 1 ) );
        events.add( roleForId( SLAVE, 2 ) );
        events.add( roleForId( SLAVE, 1 ) );
        events.add( roleForId( SLAVE, 3 ) );
        // and this expected outcome
        List<MemberIsAvailable> expected = new LinkedList<MemberIsAvailable>();
        expected.add( roleForId( SLAVE, 2 ) );
        expected.add( roleForId( SLAVE, 1 ) );
        expected.add( roleForId( SLAVE, 3 ) );

        // WHEN events start getting added
        Iterable<MemberIsAvailable > result = new LinkedList<MemberIsAvailable>();
        for ( MemberIsAvailable event : events )
        {
            result = new HANewSnapshotFunction().apply( result, event );
        }

        // THEN the result should be the expected one
        eventsMatch( result, expected );
    }

    @Test
    public void instanceBeingSlaveReappearsAsMasterShouldBeTreatedAsMaster() throws Exception
    {
        // GIVEN these events
        List<MemberIsAvailable> events = new LinkedList<MemberIsAvailable>();
        events.add( roleForId( SLAVE, 2 ) );
        events.add( roleForId( SLAVE, 1 ) );
        events.add( roleForId( MASTER, 1 ) );
        events.add( roleForId( SLAVE, 3 ) );
        // and this expected outcome
        List<MemberIsAvailable> expected = new LinkedList<MemberIsAvailable>();
        expected.add( roleForId( SLAVE, 2 ) );
        expected.add( roleForId( MASTER, 1 ) );
        expected.add( roleForId( SLAVE, 3 ) );

        // WHEN events start getting added
        Iterable<MemberIsAvailable > result = new LinkedList<MemberIsAvailable>();
        for ( MemberIsAvailable event : events )
        {
            result = new HANewSnapshotFunction().apply( result, event );
        }

        // THEN the result should be the expected one
        eventsMatch( result, expected );
    }

    @Test
    public void instanceBeingMasterReplacedByAnotherInstanceShouldNotRemainMaster() throws Exception
    {
        // GIVEN these events
        List<MemberIsAvailable> events = new LinkedList<MemberIsAvailable>();
        events.add( roleForId( MASTER, 1 ) );
        events.add( roleForId( BACKUP, 1 ) );
        events.add( roleForId( MASTER, 2 ) );
        events.add( roleForId( SLAVE, 3 ) );
        // and this expected outcome
        List<MemberIsAvailable> expected = new LinkedList<MemberIsAvailable>();
        expected.add( roleForId( MASTER, 2 ) );
        expected.add( roleForId( SLAVE, 3 ) );

        // WHEN events start getting added
        Iterable<MemberIsAvailable > result = new LinkedList<MemberIsAvailable>();
        for ( MemberIsAvailable event : events )
        {
            result = new HANewSnapshotFunction().apply( result, event );
        }

        // THEN the result should be the expected one
        eventsMatch( result, expected );
    }

    private MemberIsAvailable roleForId( String role, int id )
    {
        return new MemberIsAvailable( role, new InstanceId( id ),
                URI.create( "cluster://"+id ), URI.create( "ha://"+id ) );
    }

    private void eventsMatch( Iterable<MemberIsAvailable> result, List<MemberIsAvailable> expected )
    {
        Iterator<MemberIsAvailable> iter = result.iterator();
        for ( int i = 0; i < expected.size(); i++ )
        {
            assertEquals( expected.get( i ), iter.next() );
        }
        assertFalse( iter.hasNext() );
    }
}
