/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.neo4j.cluster.protocol.election.ElectionCredentials;
import org.neo4j.kernel.ha.cluster.DefaultElectionCredentials;

public class DefaultElectionCredentialsTest
{
    @Test
    public void testCompareToDifferentTxId() throws Exception
    {
        DefaultElectionCredentials highTxId =
                new DefaultElectionCredentials( 3, 12, false );

        DefaultElectionCredentials mediumTxId =
                new DefaultElectionCredentials( 1, 11, false );

        DefaultElectionCredentials lowTxId =
                new DefaultElectionCredentials( 2, 10, false );

        List<ElectionCredentials> toSort = new ArrayList<ElectionCredentials>( 2 );
        toSort.add( mediumTxId);
        toSort.add( highTxId);
        toSort.add( lowTxId );
        Collections.sort( toSort );
        assertEquals(toSort.get( 0 ), lowTxId);
        assertEquals(toSort.get( 1 ), mediumTxId);
        assertEquals(toSort.get( 2 ), highTxId);
    }

    @Test
    public void testCompareToSameTxId() throws Exception
    {
        // Lower id means higher priority
        DefaultElectionCredentials highSameTxId = new DefaultElectionCredentials( 1, 10, false );

        DefaultElectionCredentials lowSameTxId = new DefaultElectionCredentials( 2, 10, false );

        List<ElectionCredentials> toSort = new ArrayList<ElectionCredentials>( 2 );
        toSort.add( highSameTxId );
        toSort.add(lowSameTxId);
        Collections.sort( toSort );
        assertEquals(toSort.get( 0 ), lowSameTxId);
        assertEquals(toSort.get( 1 ), highSameTxId);
    }

    @Test
    public void testExistingMasterLosesWhenComparedToHigherTxIdHigherId()
    {
        DefaultElectionCredentials currentMaster = new DefaultElectionCredentials( 1, 10, true );
        DefaultElectionCredentials incoming = new DefaultElectionCredentials( 2, 11, false );

        List<ElectionCredentials> toSort = new ArrayList<ElectionCredentials>( 2 );
        toSort.add( currentMaster );
        toSort.add( incoming );
        Collections.sort( toSort );

        assertEquals( toSort.get( 0 ), currentMaster );
        assertEquals( toSort.get( 1 ), incoming );
    }

    @Test
    public void testExistingMasterWinsWhenComparedToLowerIdSameTxId()
    {
        DefaultElectionCredentials currentMaster = new DefaultElectionCredentials( 2, 10, true );
        DefaultElectionCredentials incoming = new DefaultElectionCredentials( 1, 10, false );

        List<ElectionCredentials> toSort = new ArrayList<ElectionCredentials>( 2 );
        toSort.add( currentMaster );
        toSort.add( incoming );
        Collections.sort( toSort );

        assertEquals( toSort.get( 0 ), incoming );
        assertEquals( toSort.get( 1 ), currentMaster );
    }

    @Test
    public void testExistingMasterWinsWhenComparedToHigherIdLowerTxId()
    {
        DefaultElectionCredentials currentMaster = new DefaultElectionCredentials( 1, 10, true );
        DefaultElectionCredentials incoming = new DefaultElectionCredentials( 2, 9, false );

        List<ElectionCredentials> toSort = new ArrayList<ElectionCredentials>( 2 );
        toSort.add( currentMaster );
        toSort.add( incoming );
        Collections.sort( toSort );

        assertEquals( toSort.get( 0 ), incoming );
        assertEquals( toSort.get( 1 ), currentMaster );
    }

    @Test
    public void testEquals() throws Exception
    {
        DefaultElectionCredentials sameAsNext =
                new DefaultElectionCredentials( 1, 10, false );

        DefaultElectionCredentials sameAsPrevious =
                new DefaultElectionCredentials( 1, 10, false );

        assertEquals( sameAsNext, sameAsPrevious );
        assertEquals( sameAsNext, sameAsNext );


        DefaultElectionCredentials differentTxIdFromNext =
                new DefaultElectionCredentials( 1, 11, false );

        DefaultElectionCredentials differentTxIdFromPrevious =
                new DefaultElectionCredentials( 1, 10, false );

        assertFalse( differentTxIdFromNext.equals( differentTxIdFromPrevious ) );
        assertFalse( differentTxIdFromPrevious.equals( differentTxIdFromNext ) );

        DefaultElectionCredentials differentURIFromNext =
                new DefaultElectionCredentials( 1, 11, false );

        DefaultElectionCredentials differentURIFromPrevious =
                new DefaultElectionCredentials( 2, 11, false );

        assertFalse( differentTxIdFromNext.equals( differentURIFromPrevious ) );
        assertFalse( differentTxIdFromPrevious.equals( differentURIFromNext ) );
    }
}
