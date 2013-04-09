/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.state;

import static junit.framework.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import org.junit.Test;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.WritableTransactionState;
import org.neo4j.kernel.impl.nioneo.store.PropertyDatas;
import org.neo4j.kernel.logging.DevNullLoggingService;

public class OldTxStateBridgeImplTest
{

    @Test
    public void shouldListNodesWithPropertyAdded() throws Exception
    {
        // Given
        long nodeId = 1l;
        int propertyKey = 2;
        int value = 1337;

        WritableTransactionState state = new WritableTransactionState( null, null, new DevNullLoggingService(), null,
                null, null );
        OldTxStateBridge bridge = new OldTxStateBridgeImpl( null, state );

        NodeImpl node = new NodeImpl( nodeId, -1, -1 );

        // And Given that I've added a relevant property
        state.getOrCreateCowPropertyAddMap( node ).put( 2, PropertyDatas.forInt( propertyKey, -1l, value ) );

        // When
        DiffSets<Long> nodes = bridge.getNodesWithChangedProperty( propertyKey, value );

        // Then
        assertEquals( asSet( nodeId ), nodes.getAdded() );
        assertEquals( asSet(), nodes.getRemoved() );
    }

    @Test
    public void shouldListNodesWithPropertyRemoved() throws Exception
    {
        // Given
        long nodeId = 1l;
        int propertyKey = 2;
        int value = 1337;

        WritableTransactionState state = new WritableTransactionState( null, null, new DevNullLoggingService(), null,
                null, null );
        OldTxStateBridge bridge = new OldTxStateBridgeImpl( null, state );

        NodeImpl node = new NodeImpl( nodeId, -1, -1 );

        // And Given that I've added a relevant property
        state.getOrCreateCowPropertyRemoveMap( node ).put( 2, PropertyDatas.forInt( propertyKey, -1l, value ) );

        // When
        DiffSets<Long> nodes = bridge.getNodesWithChangedProperty( propertyKey, value );

        // Then
        assertEquals( asSet(), nodes.getAdded() );
        assertEquals( asSet( nodeId ), nodes.getRemoved() );
    }

    @Test
    public void shouldListNodesWithPropertyChanged() throws Exception
    {
        // Given
        long nodeId = 1l;
        int propertyKey = 2;
        int value = 1337;

        WritableTransactionState state = new WritableTransactionState( null, null, new DevNullLoggingService(), null,
                null, null );
        OldTxStateBridge bridge = new OldTxStateBridgeImpl( null, state );

        NodeImpl node = new NodeImpl( nodeId, -1, -1 );

        // And Given that I've added a relevant property
        state.getOrCreateCowPropertyAddMap( node ).put( 2, PropertyDatas.forInt( propertyKey, -1l,
        /*other value*/7331 ) );

        // When
        DiffSets<Long> nodes = bridge.getNodesWithChangedProperty( propertyKey, value );

        // Then
        assertEquals( asSet(), nodes.getAdded() );
        assertEquals( asSet( nodeId ), nodes.getRemoved() );
    }

}
