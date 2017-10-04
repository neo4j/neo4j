/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;

public abstract class RuntimeITBase
{
    abstract Runtime runtime();

    final int PROP = 1000;
    final int LABEL = 1000;
    final int IRRELEVANT_LABEL = -1000;

    @Test
    public void shouldDoNodeAllScan()
    {
        // given
        Runtime r = runtime();
        long n1 = createNode( r );
        long n2 = createNode( r );
        long n3 = createNode( r );

        // when
        NodeCursor cursor = r.cursorFactory().allocateNodeCursor();
        r.read().allNodesScan( cursor );

        List<Long> scannedSet = new ArrayList<>();
        while ( cursor.next() )
        {
            scannedSet.add( cursor.nodeReference() );
            assertFalse( cursor.hasProperties() );
            assertThat( cursor.labels().numberOfLabels(), equalTo( 0 ) );
        }

        // then
        assertThat( scannedSet, containsInAnyOrder( n1, n2, n3 ) );
    }

    @Test
    public void shouldDoNodeLabelScan()
    {
        // given
        Runtime r = runtime();
        createNode( r );
        long n1 = createNode( r, LABEL );
        long n2 = createNode( r, LABEL );
        createNode( r );
        createNode( r, IRRELEVANT_LABEL );
        long n3 = createNode( r, LABEL );
        createNode( r );

        // when
        NodeLabelIndexCursor cursor = r.cursorFactory().allocateNodeLabelIndexCursor();
        r.read().nodeLabelScan( LABEL, cursor );

        List<Long> scannedSet = new ArrayList<>();
        while ( cursor.next() )
        {
            scannedSet.add( cursor.nodeReference() );
            assertThat( cursor.labels().numberOfLabels(), equalTo( 1 ) );
            assertThat( cursor.labels().label( 0 ), equalTo( LABEL ) );
        }

        // then
        assertThat( scannedSet, containsInAnyOrder( n1, n2, n3 ) );
    }

    @Test
    public void shouldDoAllRelationshipScan()
    {
        // given
        Runtime r = runtime();
        long n1 = createNode( r );
        long n2 = createNode( r );
        long n3 = createNode( r );

        r.write().relationshipCreate( n1, LABEL, n2 );
        r.write().relationshipCreate( n2, LABEL, n3 );
        r.write().relationshipCreate( n3, LABEL, n1 );

        // when
        RelationshipScanCursor cursor = r.cursorFactory().allocateRelationshipScanCursor();
        r.read().allRelationshipsScan( cursor );

        List<Relationship> scannedSet = new ArrayList<>();
        while ( cursor.next() )
        {
            scannedSet.add( Relationship.asDefined( cursor ) );
        }

        // then
        assertThat(
                scannedSet,
                containsInAnyOrder(
                        new Relationship( n1, LABEL, n2 ),
                        new Relationship( n2, LABEL, n3 ),
                        new Relationship( n3, LABEL, n1 )
                ) );
    }

    @Test
    public void shouldExpandByRelationshipGroup()
    {
        // given
        Runtime r = runtime();
        long n1 = createNode( r );
        long n2 = createNode( r );
        long n3 = createNode( r );

        int label1 = 11;
        int label2 = 22;
        int label3 = 33;

        r.write().relationshipCreate( n1, label1, n2 );
        r.write().relationshipCreate( n1, label1, n2 );

        r.write().relationshipCreate( n2, label1, n3 );
        r.write().relationshipCreate( n2, label2, n3 );
        r.write().relationshipCreate( n2, label2, n3 );

        r.write().relationshipCreate( n3, label3, n1 );
        r.write().relationshipCreate( n3, label3, n1 );
        r.write().relationshipCreate( n3, label2, n1 );
        r.write().relationshipCreate( n3, label1, n1 );
        r.write().relationshipCreate( n3, label1, n1 );

        // when
        NodeCursor nodeCursor = r.cursorFactory().allocateNodeCursor();
        RelationshipTraversalCursor relationshipCursor = r.cursorFactory().allocateRelationshipTraversalCursor();
        RelationshipGroupCursor groupCursor = r.cursorFactory().allocateRelationshipGroupCursor();
        r.read().allNodesScan( nodeCursor );

        List<Relationship> scannedSet = new ArrayList<>();
        while ( nodeCursor.next() )
        {
            nodeCursor.relationships( groupCursor );
            while ( groupCursor.next() )
            {
                int groupLabel = groupCursor.relationshipLabel();
                for ( int dir = 0; dir < 3; dir++ )
                {
                    if ( dir == 0 )
                    {
                        groupCursor.outgoing( relationshipCursor );
                    }
                    else if ( dir == 1 )
                    {
                        groupCursor.incoming( relationshipCursor );
                    }
                    else
                    {
                        groupCursor.loops( relationshipCursor );
                    }
                    while ( relationshipCursor.next() )
                    {
                        assertThat( relationshipCursor.label(), equalTo( groupLabel ) );
                        scannedSet.add( Relationship.asTraversed( relationshipCursor ) );
                    }
                }
            }
        }

        // then
        assertThat(
                scannedSet,
                containsInAnyOrder(
                        // forward direction
                        new Relationship( n1, label1, n2 ),
                        new Relationship( n1, label1, n2 ),

                        new Relationship( n2, label1, n3 ),
                        new Relationship( n2, label2, n3 ),
                        new Relationship( n2, label2, n3 ),

                        new Relationship( n3, label3, n1 ),
                        new Relationship( n3, label3, n1 ),
                        new Relationship( n3, label2, n1 ),
                        new Relationship( n3, label1, n1 ),
                        new Relationship( n3, label1, n1 ),

                        // backward direction
                        new Relationship( n2, label1, n1 ),
                        new Relationship( n2, label1, n1 ),

                        new Relationship( n3, label1, n2 ),
                        new Relationship( n3, label2, n2 ),
                        new Relationship( n3, label2, n2 ),

                        new Relationship( n1, label3, n3 ),
                        new Relationship( n1, label3, n3 ),
                        new Relationship( n1, label2, n3 ),
                        new Relationship( n1, label1, n3 ),
                        new Relationship( n1, label1, n3 )
                ) );
    }

    private long createNode( Runtime r, int... labels )
    {
        long nodeId = r.write().nodeCreate();
        for ( int label : labels )
        {
            r.write().nodeAddLabel( nodeId, label );
        }
        return nodeId;
    }

    static class Relationship
    {
        final long from;
        final int label;
        final long to;

        Relationship( long from, int label, long to )
        {
            this.from = from;
            this.label = label;
            this.to = to;
        }

        static Relationship asDefined( RelationshipScanCursor cursor )
        {
            return new Relationship( cursor.sourceNodeReference(), cursor.label(), cursor.targetNodeReference() );
        }

        static Relationship asTraversed( RelationshipTraversalCursor cursor )
        {
            long neighbourNodeRef = cursor.neighbourNodeReference();
            long homeNodeRef = cursor.sourceNodeReference() == neighbourNodeRef ?
                    cursor.targetNodeReference() : cursor.sourceNodeReference();
            return new Relationship( homeNodeRef, cursor.label(), neighbourNodeRef );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Relationship relationship = (Relationship) o;

            if ( from != relationship.from )
            {
                return false;
            }
            if ( to != relationship.to )
            {
                return false;
            }
            return label == relationship.label;
        }

        @Override
        public int hashCode()
        {
            int result = (int) (from ^ (from >>> 32));
            result = 31 * result + (int) (to ^ (to >>> 32));
            result = 31 * result + label;
            return result;
        }
    }
}
