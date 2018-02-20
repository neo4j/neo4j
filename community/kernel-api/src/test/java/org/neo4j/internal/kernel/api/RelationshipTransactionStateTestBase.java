/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.internal.kernel.api.RelationshipTestSupport.assertCount;
import static org.neo4j.internal.kernel.api.RelationshipTestSupport.assertCounts;
import static org.neo4j.internal.kernel.api.RelationshipTestSupport.computeKey;
import static org.neo4j.internal.kernel.api.RelationshipTestSupport.count;
import static org.neo4j.internal.kernel.api.RelationshipTestSupport.sparse;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.stringValue;

@SuppressWarnings( "Duplicates" )
public abstract class RelationshipTransactionStateTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{
    @Test
    public void shouldSeeSingleRelationshipInTransaction() throws Exception
    {
        int label;
        long n1, n2;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();

            // setup extra relationship to challenge the implementation
            long decoyNode = tx.dataWrite().nodeCreate();
            label = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            tx.dataWrite().relationshipCreate( n2, label, decoyNode );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            long r = tx.dataWrite().relationshipCreate( n1, label, n2 );
            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor() )
            {
                tx.dataRead().singleRelationship( r, relationship );
                assertTrue( "should find relationship", relationship.next() );

                assertEquals( label, relationship.label() );
                assertEquals( n1, relationship.sourceNodeReference() );
                assertEquals( n2, relationship.targetNodeReference() );
                assertEquals( r, relationship.relationshipReference() );

                assertFalse( "should only find one relationship", relationship.next() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldNotSeeSingleRelationshipWhichWasDeletedInTransaction() throws Exception
    {
        int label;
        long n1, n2, r;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            label = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );

            long decoyNode = tx.dataWrite().nodeCreate();
            tx.dataWrite().relationshipCreate( n2, label, decoyNode ); // to have >1 relationship in the db

            r = tx.dataWrite().relationshipCreate( n1, label, n2 );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            assertTrue( "should delete relationship", tx.dataWrite().relationshipDelete( r ) );
            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor() )
            {
                tx.dataRead().singleRelationship( r, relationship );
                assertFalse( "should not find relationship", relationship.next() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldScanRelationshipInTransaction() throws Exception
    {
        final int nRelationshipsInStore = 10;

        int type;
        long n1, n2;

        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();

            // setup some in store relationships
            type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            relateNTimes( nRelationshipsInStore, type, n1, n2, tx );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            long r = tx.dataWrite().relationshipCreate( n1, type, n2 );
            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor() )
            {
                tx.dataRead().allRelationshipsScan( relationship );
                assertCountRelationships( relationship, nRelationshipsInStore + 1, n1, type, n2 );
            }
            tx.success();
        }
    }

    @Test
    public void shouldNotScanRelationshipWhichWasDeletedInTransaction() throws Exception
    {
        final int nRelationshipsInStore = 5 + 1 + 5;

        int type;
        long n1, n2, r;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );

            relateNTimes( 5, type, n1, n2, tx );
            r = tx.dataWrite().relationshipCreate( n1, type, n2 );
            relateNTimes( 5, type, n1, n2, tx );

            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            assertTrue( "should delete relationship", tx.dataWrite().relationshipDelete( r ) );
            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor() )
            {
                tx.dataRead().allRelationshipsScan( relationship );
                assertCountRelationships( relationship, nRelationshipsInStore - 1, n1, type, n2 );
            }
            tx.success();
        }
    }

    @Test
    public void shouldSeeRelationshipInTransaction() throws Exception
    {
        long n1, n2;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            int label = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            long r = tx.dataWrite().relationshipCreate( n1, label, n2 );
            try ( NodeCursor node = cursors.allocateNodeCursor();
                  RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
            {
                tx.dataRead().singleNode( n1, node );
                assertTrue( "should access node", node.next() );

                node.allRelationships( relationship );
                assertTrue( "should find relationship", relationship.next() );
                assertEquals( r, relationship.relationshipReference() );

                assertFalse( "should only find one relationship", relationship.next() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldNotSeeRelationshipDeletedInTransaction() throws Exception
    {
        long n1, n2, r;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();

            int label = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            r = tx.dataWrite().relationshipCreate( n1, label, n2 );

            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            tx.dataWrite().relationshipDelete( r );
            try ( NodeCursor node = cursors.allocateNodeCursor();
                  RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
            {
                tx.dataRead().singleNode( n1, node );
                assertTrue( "should access node", node.next() );

                node.allRelationships( relationship );
                assertFalse( "should not find relationship", relationship.next() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldSeeRelationshipInTransactionBeforeCursorInitialization() throws Exception
    {
        long n1, n2;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            int label = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            long r = tx.dataWrite().relationshipCreate( n1, label, n2 );
            try ( NodeCursor node = cursors.allocateNodeCursor();
                  RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
            {
                tx.dataRead().singleNode( n1, node );
                assertTrue( "should access node", node.next() );

                node.allRelationships( relationship );
                assertTrue( "should find relationship", relationship.next() );
                assertEquals( r, relationship.relationshipReference() );

                tx.dataWrite().relationshipCreate( n1, label, n2 ); // should not be seen
                assertFalse( "should not find relationship added after cursor init", relationship.next() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldTraverseSparseNodeWithoutGroups() throws Exception
    {
        traverseWithoutGroups( sparse( graphDb ), false );
    }

    @Test
    public void shouldTraverseDenseNodeWithoutGroups() throws Exception
    {
        traverseWithoutGroups( RelationshipTestSupport.dense( graphDb ), false );
    }

    @Test
    public void shouldTraverseSparseNodeWithoutGroupsWithDetachedReferences() throws Exception
    {
        traverseWithoutGroups( sparse( graphDb ), true );
    }

    @Test
    public void shouldTraverseDenseNodeWithoutGroupsWithDetachedReferences() throws Exception
    {
        traverseWithoutGroups( RelationshipTestSupport.dense( graphDb ), true );
    }

    @Test
    public void shouldTraverseSparseNodeViaGroups() throws Exception
    {
        traverseViaGroups( sparse( graphDb ), false );
    }

    @Test
    public void shouldTraverseDenseNodeViaGroups() throws Exception
    {
        traverseViaGroups( RelationshipTestSupport.dense( graphDb ), false );
    }

    @Test
    public void shouldTraverseSparseNodeViaGroupsWithDetachedReferences() throws Exception
    {
        traverseViaGroups( sparse( graphDb ), true );
    }

    @Test
    public void shouldTraverseDenseNodeViaGroupsWithDetachedReferences() throws Exception
    {
        traverseViaGroups( RelationshipTestSupport.dense( graphDb ), true );
    }

    @Test
    public void shouldSeeNewRelationshipPropertyInTransaction() throws Exception
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            String propKey1 = "prop1";
            String propKey2 = "prop2";
            long n1 = tx.dataWrite().nodeCreate();
            long n2 = tx.dataWrite().nodeCreate();
            int label = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            long r = tx.dataWrite().relationshipCreate( n1, label, n2 );
            int prop1 = session.token().propertyKeyGetOrCreateForName( propKey1 );
            int prop2 = session.token().propertyKeyGetOrCreateForName( propKey2 );
            assertEquals( tx.dataWrite().relationshipSetProperty( r, prop1, stringValue( "hello" ) ), NO_VALUE );
            assertEquals( tx.dataWrite().relationshipSetProperty( r, prop2, stringValue( "world" ) ), NO_VALUE );

            try ( NodeCursor node = cursors.allocateNodeCursor();
                  RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor();
                  PropertyCursor property = cursors.allocatePropertyCursor() )
            {
                tx.dataRead().singleNode( n1, node );
                assertTrue( "should access node", node.next() );
                node.allRelationships( relationship );

                assertTrue( "should access relationship", relationship.next() );

                relationship.properties( property );

                while ( property.next() )
                {
                    if ( property.propertyKey() == prop1 )
                    {
                        assertEquals( property.propertyValue(), stringValue( "hello" ) );
                    }
                    else if ( property.propertyKey() == prop2 )
                    {
                        assertEquals( property.propertyValue(), stringValue( "world" ) );
                    }
                    else
                    {
                        fail( property.propertyKey() + " was not the property key you were looking for" );
                    }
                }

                assertFalse( "should only find one relationship", relationship.next() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldSeeAddedPropertyFromExistingRelationshipWithoutPropertiesInTransaction() throws Exception
    {
        // Given
        long relationshipId;
        String propKey = "prop1";
        try ( Transaction tx = session.beginTransaction() )
        {
            Write write = tx.dataWrite();
            relationshipId = write.relationshipCreate( write.nodeCreate(),
                    tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" ), write.nodeCreate() );
            tx.success();
        }

        // When/Then
        try ( Transaction tx = session.beginTransaction() )
        {
            int propToken = session.token().propertyKeyGetOrCreateForName( propKey );
            assertEquals( tx.dataWrite().relationshipSetProperty( relationshipId, propToken, stringValue( "hello" ) ),
                    NO_VALUE );

            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor();
                  PropertyCursor property = cursors.allocatePropertyCursor() )
            {
                tx.dataRead().singleRelationship( relationshipId, relationship );
                assertTrue( "should access relationship", relationship.next() );

                relationship.properties( property );
                assertTrue( property.next() );
                assertEquals( propToken, property.propertyKey() );
                assertEquals( property.propertyValue(), stringValue( "hello" ) );

                assertFalse( "should only find one properties", property.next() );
                assertFalse( "should only find one relationship", relationship.next() );
            }

            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignored = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getRelationshipById( relationshipId ).getProperty( propKey ), equalTo( "hello" ) );
        }
    }

    @Test
    public void shouldSeeAddedPropertyFromExistingRelationshipWithPropertiesInTransaction() throws Exception
    {
        // Given
        long relationshipId;
        String propKey1 = "prop1";
        String propKey2 = "prop2";
        int propToken1;
        int propToken2;
        try ( Transaction tx = session.beginTransaction() )
        {
            Write write = tx.dataWrite();
            relationshipId = write.relationshipCreate( write.nodeCreate(),
                    tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" ), write.nodeCreate() );
            propToken1 = session.token().propertyKeyGetOrCreateForName( propKey1 );
            assertEquals( write.relationshipSetProperty( relationshipId, propToken1, stringValue( "hello" ) ),
                    NO_VALUE );
            tx.success();
        }

        // When/Then
        try ( Transaction tx = session.beginTransaction() )
        {
            propToken2 = session.token().propertyKeyGetOrCreateForName( propKey2 );
            assertEquals( tx.dataWrite().relationshipSetProperty( relationshipId, propToken2, stringValue( "world" ) ),
                    NO_VALUE );

            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor();
                  PropertyCursor property = cursors.allocatePropertyCursor() )
            {
                tx.dataRead().singleRelationship( relationshipId, relationship );
                assertTrue( "should access relationship", relationship.next() );

                relationship.properties( property );

                while ( property.next() )
                {
                    if ( property.propertyKey() == propToken1 )//from disk
                    {
                        assertEquals( property.propertyValue(), stringValue( "hello" ) );

                    }
                    else if ( property.propertyKey() == propToken2 )//from tx state
                    {
                        assertEquals( property.propertyValue(), stringValue( "world" ) );
                    }
                    else
                    {
                        fail( property.propertyKey() + " was not the property you were looking for" );
                    }
                }

                assertFalse( "should only find one relationship", relationship.next() );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignored = graphDb.beginTx() )
        {
            Relationship relationship = graphDb.getRelationshipById( relationshipId );
            assertThat( relationship.getProperty( propKey1 ), equalTo( "hello" ) );
            assertThat( relationship.getProperty( propKey2 ), equalTo( "world" ) );
        }
    }

    @Test
    public void shouldSeeUpdatedPropertyFromExistingRelationshipWithPropertiesInTransaction() throws Exception
    {
        // Given
        long relationshipId;
        String propKey = "prop1";
        int propToken;
        try ( Transaction tx = session.beginTransaction() )
        {
            Write write = tx.dataWrite();
            relationshipId = write.relationshipCreate( write.nodeCreate(),
                    tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" ), write.nodeCreate() );
            propToken = session.token().propertyKeyGetOrCreateForName( propKey );
            assertEquals( write.relationshipSetProperty( relationshipId, propToken, stringValue( "hello" ) ),
                    NO_VALUE );
            tx.success();
        }

        // When/Then
        try ( Transaction tx = session.beginTransaction() )
        {
            assertEquals( tx.dataWrite().relationshipSetProperty( relationshipId, propToken, stringValue( "world" ) ),
                    stringValue( "hello" ) );
            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor();
                  PropertyCursor property = cursors.allocatePropertyCursor() )
            {
                tx.dataRead().singleRelationship( relationshipId, relationship );
                assertTrue( "should access relationship", relationship.next() );

                relationship.properties( property );

                assertTrue( property.next() );
                assertEquals( propToken, property.propertyKey() );
                assertEquals( property.propertyValue(), stringValue( "world" ) );

                assertFalse( "should only find one property", property.next() );
                assertFalse( "should only find one relationship", relationship.next() );
            }

            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignored = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getRelationshipById( relationshipId ).getProperty( propKey ), equalTo( "world" ) );
        }
    }

    @Test
    public void shouldNotSeeRemovedPropertyInTransaction() throws Exception
    {
        // Given
        long relationshipId;
        String propKey = "prop1";
        int propToken;
        try ( Transaction tx = session.beginTransaction() )
        {
            Write write = tx.dataWrite();
            relationshipId = write.relationshipCreate( write.nodeCreate(),
                    tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" ), write.nodeCreate() );
            propToken = session.token().propertyKeyGetOrCreateForName( propKey );
            assertEquals( write.relationshipSetProperty( relationshipId, propToken, stringValue( "hello" ) ),
                    NO_VALUE );
            tx.success();
        }

        // When/Then
        try ( Transaction tx = session.beginTransaction() )
        {
            assertEquals( tx.dataWrite().relationshipRemoveProperty( relationshipId, propToken ),
                    stringValue( "hello" ) );
            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor();
                  PropertyCursor property = cursors.allocatePropertyCursor() )
            {
                tx.dataRead().singleRelationship( relationshipId, relationship );
                assertTrue( "should access relationship", relationship.next() );

                relationship.properties( property );
                assertFalse( "should not find any properties", property.next() );
                assertFalse( "should only find one relationship", relationship.next() );
            }

            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignored = graphDb.beginTx() )
        {
            assertFalse(
                    graphDb.getRelationshipById( relationshipId ).hasProperty( propKey ) );
        }
    }

    @Test
    public void shouldSeeRemovedThenAddedPropertyInTransaction() throws Exception
    {
        // Given
        long relationshipId;
        String propKey = "prop1";
        int propToken;
        try ( Transaction tx = session.beginTransaction() )
        {
            Write write = tx.dataWrite();
            relationshipId = write.relationshipCreate( write.nodeCreate(),
                    tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" ), write.nodeCreate() );
            propToken = session.token().propertyKeyGetOrCreateForName( propKey );
            assertEquals( write.relationshipSetProperty( relationshipId, propToken, stringValue( "hello" ) ),
                    NO_VALUE );
            tx.success();
        }

        // When/Then
        try ( Transaction tx = session.beginTransaction() )
        {
            assertEquals( tx.dataWrite().relationshipRemoveProperty( relationshipId, propToken ),
                    stringValue( "hello" ) );
            assertEquals( tx.dataWrite().relationshipSetProperty( relationshipId, propToken, stringValue( "world" ) ),
                    NO_VALUE );
            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor();
                  PropertyCursor property = cursors.allocatePropertyCursor() )
            {
                tx.dataRead().singleRelationship( relationshipId, relationship );
                assertTrue( "should access relationship", relationship.next() );

                relationship.properties( property );
                assertTrue( property.next() );
                assertEquals( propToken, property.propertyKey() );
                assertEquals( property.propertyValue(), stringValue( "world" ) );

                assertFalse( "should not find any properties", property.next() );
                assertFalse( "should only find one relationship", relationship.next() );
            }

            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignored = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getRelationshipById( relationshipId ).getProperty( propKey ), equalTo( "world" ) );
        }
    }


    @Test
    public void shouldCountOutgoingNodesFromTxState() throws Exception
    {
        assertOutgoingCount( 100 );//node will be dense
        assertOutgoingCount( 1 );//node will be sparse
    }

    private void traverseWithoutGroups( RelationshipTestSupport.StartNode start, boolean detached ) throws Exception
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            Map<String,Integer> expectedCounts = modifyStartNodeRelationships( start, tx );

            // given
            try ( NodeCursor node = cursors.allocateNodeCursor();
                  RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
            {
                // when
                tx.dataRead().singleNode( start.id, node );

                assertTrue( "access node", node.next() );
                if ( detached )
                {
                    tx.dataRead().relationships( start.id, node.allRelationshipsReference(), relationship );
                }
                else
                {
                    node.allRelationships( relationship );
                }

                Map<String,Integer> counts = count( session, relationship );

                // then
                assertCounts( expectedCounts, counts );
            }

            tx.failure();
        }
    }

    private void traverseViaGroups( RelationshipTestSupport.StartNode start, boolean detached ) throws Exception
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            Read read = tx.dataRead();
            Map<String,Integer> expectedCounts = modifyStartNodeRelationships( start, tx );

            // given
            try ( NodeCursor node = cursors.allocateNodeCursor();
                  RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
                  RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
            {
                // when
                read.singleNode( start.id, node );
                assertTrue( "access node", node.next() );
                if ( detached )
                {
                    read.relationshipGroups( start.id, node.relationshipGroupReference(), group );
                }
                else
                {
                    node.relationships( group );
                }

                while ( group.next() )
                {
                    // outgoing
                    if ( detached )
                    {
                        read.relationships( start.id, group.outgoingReference(), relationship );
                    }
                    else
                    {
                        group.outgoing( relationship );
                    }
                    // then
                    assertCount( session, relationship, expectedCounts, group.relationshipLabel(), OUTGOING );

                    // incoming
                    if ( detached )
                    {
                        read.relationships( start.id, group.incomingReference(), relationship );
                    }
                    else
                    {
                        group.incoming( relationship );
                    }
                    // then
                    assertCount( session, relationship, expectedCounts, group.relationshipLabel(), INCOMING );

                    // loops
                    if ( detached )
                    {
                        read.relationships( start.id, group.loopsReference(), relationship );
                    }
                    else
                    {
                        group.loops( relationship );
                    }
                    // then
                    assertCount( session, relationship, expectedCounts, group.relationshipLabel(), BOTH );
                }
            }
        }
    }

    private Map<String,Integer> modifyStartNodeRelationships( RelationshipTestSupport.StartNode start, Transaction tx )
            throws KernelException
    {
        Map<String,Integer> expectedCounts = new HashMap<>();
        for ( Map.Entry<String,List<RelationshipTestSupport.StartRelationship>> kv : start.relationships.entrySet() )
        {
            List<RelationshipTestSupport.StartRelationship> rs = kv.getValue();
            RelationshipTestSupport.StartRelationship head = rs.get( 0 );
            int type = session.token().relationshipType( head.type.name() );
            switch ( head.direction )
            {
            case INCOMING:
                tx.dataWrite().relationshipCreate( tx.dataWrite().nodeCreate(), type, start.id );
                tx.dataWrite().relationshipCreate( tx.dataWrite().nodeCreate(), type, start.id );
                break;
            case OUTGOING:
                tx.dataWrite().relationshipCreate( start.id, type, tx.dataWrite().nodeCreate() );
                tx.dataWrite().relationshipCreate( start.id, type, tx.dataWrite().nodeCreate() );
                break;
            case BOTH:
                tx.dataWrite().relationshipCreate( start.id, type, start.id );
                tx.dataWrite().relationshipCreate( start.id, type, start.id );
                break;
            default:
                throw new IllegalStateException( "Oh ye be cursed, foul checkstyle!" );
            }
            tx.dataWrite().relationshipDelete( head.id );
            expectedCounts.put( kv.getKey(), rs.size() + 1 );
        }

        String newTypeName = "NEW";
        int newType = session.token().relationshipTypeGetOrCreateForName( newTypeName );
        tx.dataWrite().relationshipCreate( tx.dataWrite().nodeCreate(), newType, start.id );
        tx.dataWrite().relationshipCreate( start.id, newType, tx.dataWrite().nodeCreate() );
        tx.dataWrite().relationshipCreate( start.id, newType, start.id );

        expectedCounts.put( computeKey( newTypeName, OUTGOING ), 1 );
        expectedCounts.put( computeKey( newTypeName, INCOMING ), 1 );
        expectedCounts.put( computeKey( newTypeName, BOTH ), 1 );

        return expectedCounts;
    }

    private void relateNTimes( int nRelationshipsInStore, int type, long n1, long n2, Transaction tx )
            throws KernelException
    {
        for ( int i = 0; i < nRelationshipsInStore; i++ )
        {
            tx.dataWrite().relationshipCreate( n1, type, n2 );
        }
    }

    private void assertCountRelationships(
            RelationshipScanCursor relationship, int expectedCount, long sourceNode, int type, long targetNode )
    {
        int count = 0;
        while ( relationship.next() )
        {
            assertEquals( sourceNode, relationship.sourceNodeReference() );
            assertEquals( type, relationship.label() );
            assertEquals( targetNode, relationship.targetNodeReference() );
            count++;
        }
        assertEquals( expectedCount, count );
    }

    private void assertOutgoingCount( int count ) throws Exception
    {
        long start;
        int type;
        try ( Transaction tx = session.beginTransaction() )
        {
            Write write = tx.dataWrite();
            start = write.nodeCreate();
            type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            for ( int i = 0; i < count; i++ )
            {
                write.relationshipCreate( start, type, write.nodeCreate() );
            }
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            Write write = tx.dataWrite();
            write.relationshipCreate( start, type, write.nodeCreate() );
            try ( NodeCursor node = cursors.allocateNodeCursor();
                  RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor() )
            {
                Read read = tx.dataRead();
                read.singleNode( start, node );
                assertTrue( node.next() );
                node.relationships( group );
                assertTrue( group.next() );

                assertEquals( count + 1, group.outgoingCount() );
                assertEquals( count + 1, group.totalCount() );
            }
        }
    }
}
