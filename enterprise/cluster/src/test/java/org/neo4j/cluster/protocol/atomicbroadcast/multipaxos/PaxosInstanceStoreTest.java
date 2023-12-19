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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.junit.Test;

public class PaxosInstanceStoreTest
{
    @Test
    public void shouldReturnSameObjectWhenAskedById()
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
    public void shouldKeepAtMostGivenNumberOfInstances()
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
    public void leaveShouldClearStoredInstances()
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
