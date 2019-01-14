/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.kernel.api;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.values.storable.Value;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

public abstract class TwoLayerTxStateTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldNotSeeCreatedNodeInStableState() throws Exception
    {
        Assume.assumeTrue( modes.twoLayerTransactionState() );

        long node;
        try ( Transaction tx = session.beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();

            try ( NodeCursor cursor = tx.cursors().allocateNodeCursor() )
            {
                tx.stableDataRead().singleNode( node, cursor );
                assertFalse( "not in stable tx state", cursor.next() );

                tx.dataRead().singleNode( node, cursor );
                assertTrue( "in active tx state", cursor.next() );
                assertEquals( node, cursor.nodeReference() );

                assertFalse( "only single node in active tx state", cursor.next() );
            }
        }
    }

    @Test
    public void shouldSeeStabilizedCreatedNodeInStableState() throws Exception
    {
        Assume.assumeTrue( modes.twoLayerTransactionState() );

        long node;
        try ( Transaction tx = session.beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();

            tx.markAsStable();

            try ( NodeCursor cursor = tx.cursors().allocateNodeCursor() )
            {
                tx.dataRead().singleNode( node, cursor );
                assertTrue( "in active tx state", cursor.next() );

                tx.stableDataRead().singleNode( node, cursor );
                assertTrue( "in stable tx state", cursor.next() );
                assertEquals( node, cursor.nodeReference() );

                assertFalse( "only single node in stable tx state", cursor.next() );
            }
        }
    }

    @Test
    public void shouldNotSeeRemovedNodeInActiveState() throws Exception
    {
        Assume.assumeTrue( modes.twoLayerTransactionState() );

        long n1;
        // given
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            // and given
            long n2 = tx.dataWrite().nodeCreate();
            long n3 = tx.dataWrite().nodeCreate();

            tx.markAsStable();

            tx.dataWrite().nodeDelete( n2 );

            // then
            assertAllNodes( tx.cursors(), tx.stableDataRead(), n1, n2, n3 );
            assertAllNodes( tx.cursors(), tx.dataRead(), n1, n3 );
        }
    }

    @Test
    public void shouldSeparateRelationshipWritesIntoLayers() throws Exception
    {
        Assume.assumeTrue( modes.twoLayerTransactionState() );

        final int TYPE = 0;
        long n1, n2, r1, r2;
        // given
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            r1 = tx.dataWrite().relationshipCreate( n1, TYPE, n2 );
            r2 = tx.dataWrite().relationshipCreate( n1, TYPE, n2 );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            // and given
            long r3 = tx.dataWrite().relationshipCreate( n1, TYPE, n2 );
            long r4 = tx.dataWrite().relationshipCreate( n1, TYPE, n2 );
            tx.dataWrite().relationshipDelete( r2 );

            tx.markAsStable();

            long r5 = tx.dataWrite().relationshipCreate( n1, TYPE, n2 );
            tx.dataWrite().relationshipDelete( r4 );

            // then
            assertExpandNode( tx.cursors(), tx.stableDataRead(), n1, r1, r3, r4 );
            assertExpandNode( tx.cursors(), tx.dataRead(), n1, r1, r3, r5 );
        }
    }

    @Test
    public void shouldSeparatePropertyWritesIntoLayers() throws Exception
    {
        Assume.assumeTrue( modes.twoLayerTransactionState() );

        final int prop1 = 0;
        final int prop2 = 1;
        final int prop3 = 2;
        final int prop4 = 2;
        final int prop5 = 2;
        final int prop6 = 2;
        long n1;

        // given
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            tx.dataWrite().nodeSetProperty( n1, prop1, intValue( 1 ) );
            tx.dataWrite().nodeSetProperty( n1, prop2, stringValue( "yo1" ) );
            tx.dataWrite().nodeSetProperty( n1, prop3, booleanValue( true ) );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            // and given
            tx.dataWrite().nodeSetProperty( n1, prop2, stringValue( "yo2" ) );
            tx.dataWrite().nodeSetProperty( n1, prop4, intValue( 2 ) );
            tx.dataWrite().nodeSetProperty( n1, prop5, stringValue( "onlyInStable" ) );
            tx.dataWrite().nodeRemoveProperty( n1, prop3 );

            tx.markAsStable();

            tx.dataWrite().nodeSetProperty( n1, prop2, stringValue( "yo3" ) );
            tx.dataWrite().nodeRemoveProperty( n1, prop5 );
            tx.dataWrite().nodeSetProperty( n1, prop6, intValue( 3 ) );

            // then
            assertNode( tx.cursors(), tx.stableDataRead(), n1 )
                    .hasProperty( prop1, intValue( 1 ) )
                    .hasProperty( prop2, stringValue( "yo2" ) )
                    .hasProperty( prop4, intValue( 2 ) )
                    .hasProperty( prop5, stringValue( "onlyInStable" ) )
                    .andNothingElse();

            assertNode( tx.cursors(), tx.dataRead(), n1 )
                    .hasProperty( prop1, intValue( 1 ) )
                    .hasProperty( prop2, stringValue( "yo3" ) )
                    .hasProperty( prop4, intValue( 2 ) )
                    .hasProperty( prop6, intValue( 3 ) )
                    .andNothingElse();
        }
    }

    private void assertAllNodes( CursorFactory cursors, Read read, long... nodes )
    {
        Set<Long> expectedSet = Arrays.stream( nodes ).boxed().collect( Collectors.toSet() );
        try ( NodeCursor node = cursors.allocateNodeCursor() )
        {
            int count = 0;
            read.allNodesScan( node );
            while ( node.next() )
            {
                assertTrue( expectedSet.contains( node.nodeReference() ) );
                count++;
            }
            assertEquals( expectedSet.size(), count );
        }
    }

    private void assertExpandNode( CursorFactory cursors, Read read, long nodeId, long... relationshipIds )
    {
        Set<Long> expectedSet = Arrays.stream( relationshipIds ).boxed().collect( Collectors.toSet() );
        try ( NodeCursor node = cursors.allocateNodeCursor();
                RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
        {
            int count = 0;
            read.singleNode( nodeId, node );
            assertTrue( node.next() );

            node.allRelationships( relationship );
            while ( relationship.next() )
            {
                assertTrue( expectedSet.contains( relationship.relationshipReference() ) );
                count++;
            }
            assertEquals( expectedSet.size(), count );
        }
    }

    private NodeProperties assertNode( CursorFactory cursors, Read read, long nodeId )
    {
        Map<Integer,Value> properties = new HashMap<>();
        try ( NodeCursor node = cursors.allocateNodeCursor();
              PropertyCursor property = cursors.allocatePropertyCursor() )
        {
            read.singleNode( nodeId, node );
            assertTrue( node.next() );

            node.properties( property );
            while ( property.next() )
            {
                properties.put( property.propertyKey(), property.propertyValue() );
            }
        }
        return new NodeProperties( properties );
    }

    static class NodeProperties
    {
        final Map<Integer,Value> properties;

        NodeProperties( Map<Integer,Value> properties )
        {
            this.properties = properties;
        }

        NodeProperties hasProperty( Integer key, Value value )
        {
            assertTrue( properties.containsKey( key ) );
            assertEquals( properties.get( key ), value );
            properties.remove( key );
            return this;
        }

        void andNothingElse()
        {
            assertTrue( properties.isEmpty() );
        }
    }
}
