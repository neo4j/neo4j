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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.junit.Test;

public class PaxosInstanceStoreTest
{
    @Test
    public void shouldReturnSameObjectWhenAskedById() throws Exception
    {
        // Given
        PaxosInstanceStore theStore = new PaxosInstanceStore();
        InstanceId currentInstanceId = new InstanceId( 1 );

        // When
        PaxosInstance currentInstance = theStore.getPaxosInstance( currentInstanceId );

        // Then
        assertSame( currentInstance, theStore.getPaxosInstance( currentInstanceId ) );
    }

    @Test
    public void shouldKeepAtMostGivenNumberOfInstances() throws Exception
    {
        // Given
        final int instancesToKeep = 10;
        PaxosInstanceStore theStore = new PaxosInstanceStore( instancesToKeep );

        // Keeps the first instance inserted, which is the first to be removed
        PaxosInstance firstInstance = null;

        // When
        for ( int i = 0; i < instancesToKeep + 1; i++ )
        {
            InstanceId currentInstanceId = new InstanceId( i );
            PaxosInstance currentInstance = theStore.getPaxosInstance( currentInstanceId );
            theStore.delivered( currentInstance.id );
            if ( firstInstance == null )
            {
                firstInstance = currentInstance;
            }
        }

        // Then
        // The first instance must have been removed now
        PaxosInstance toTest = theStore.getPaxosInstance( firstInstance.id );
        assertNotSame( firstInstance, toTest );
    }

    @Test
    public void leaveShouldClearStoredInstances() throws Exception
    {
        // Given
        PaxosInstanceStore theStore = new PaxosInstanceStore();
        InstanceId currentInstanceId = new InstanceId( 1 );

        // When
        PaxosInstance currentInstance = theStore.getPaxosInstance( currentInstanceId );
        theStore.leave();

        // Then
        assertNotSame( currentInstance, theStore.getPaxosInstance( currentInstanceId ) );
    }
}
