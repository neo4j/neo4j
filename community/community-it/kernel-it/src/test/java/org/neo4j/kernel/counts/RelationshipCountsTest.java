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
package org.neo4j.kernel.counts;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Barrier;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

@ImpermanentDbmsExtension
class RelationshipCountsTest
{
    @Inject
    private GraphDatabaseAPI db;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    void tearDown()
    {
        executor.shutdown();
    }

    @Test
    void shouldReportNumberOfRelationshipsInAnEmptyGraph()
    {
        // when
        long relationshipCount = numberOfRelationships();

        // then
        assertEquals( 0, relationshipCount );
    }

    @Test
    void shouldReportTotalNumberOfRelationships()
    {
        // given
        long before = numberOfRelationships();
        long during;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            node.createRelationshipTo( tx.createNode(), withName( "KNOWS" ) );
            node.createRelationshipTo( tx.createNode(), withName( "KNOWS" ) );
            node.createRelationshipTo( tx.createNode(), withName( "KNOWS" ) );
            during = countsForRelationship( tx, null, null, null );
            tx.commit();
        }

        // when
        long after = numberOfRelationships();

        // then
        assertEquals( 0, before );
        assertEquals( 3, during );
        assertEquals( 3, after );
    }

    @Test
    void shouldAccountForDeletedRelationships()
    {
        // given
        Relationship rel;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            node.createRelationshipTo( tx.createNode(), withName( "KNOWS" ) );
            rel = node.createRelationshipTo( tx.createNode(), withName( "KNOWS" ) );
            node.createRelationshipTo( tx.createNode(), withName( "KNOWS" ) );
            tx.commit();
        }
        long before = numberOfRelationships();
        long during;
        try ( Transaction tx = db.beginTx() )
        {
            tx.getRelationshipById( rel.getId() ).delete();
            during = countsForRelationship( tx, null, null, null );
            tx.commit();
        }

        // when
        long after = numberOfRelationships();

        // then
        assertEquals( 3, before );
        assertEquals( 2, during );
        assertEquals( 2, after );
    }

    @Test
    void shouldNotCountRelationshipsCreatedInOtherTransaction() throws Exception
    {
        // given
        final Barrier.Control barrier = new Barrier.Control();
        long before = numberOfRelationships();
        Future<Long> tx = executor.submit( () ->
        {
            try ( Transaction txn = db.beginTx() )
            {
                Node node = txn.createNode();
                node.createRelationshipTo( txn.createNode(), withName( "KNOWS" ) );
                node.createRelationshipTo( txn.createNode(), withName( "KNOWS" ) );
                long whatThisThreadSees = countsForRelationship( txn, null, null, null );
                barrier.reached();
                txn.commit();
                return whatThisThreadSees;
            }
        } );
        barrier.await();

        // when
        long during = numberOfRelationships();
        barrier.release();
        long whatOtherThreadSees = tx.get();
        long after = numberOfRelationships();

        // then
        assertEquals( 0, before );
        assertEquals( 0, during );
        assertEquals( 2, after );
        assertEquals( after, whatOtherThreadSees );
    }

    @Test
    void shouldNotCountRelationshipsDeletedInOtherTransaction() throws Exception
    {
        // given
        final Relationship rel;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            node.createRelationshipTo( tx.createNode(), withName( "KNOWS" ) );
            rel = node.createRelationshipTo( tx.createNode(), withName( "KNOWS" ) );
            node.createRelationshipTo( tx.createNode(), withName( "KNOWS" ) );
            tx.commit();
        }
        final Barrier.Control barrier = new Barrier.Control();
        long before = numberOfRelationships();
        Future<Long> tx = executor.submit( () ->
        {
            try ( Transaction txn = db.beginTx() )
            {
                txn.getRelationshipById( rel.getId() ).delete();
                long whatThisThreadSees = countsForRelationship( txn, null, null, null );
                barrier.reached();
                txn.commit();
                return whatThisThreadSees;
            }
        } );
        barrier.await();

        // when
        long during = numberOfRelationships();
        barrier.release();
        long whatOtherThreadSees = tx.get();
        long after = numberOfRelationships();

        // then
        assertEquals( 3, before );
        assertEquals( 3, during );
        assertEquals( 2, after );
        assertEquals( after, whatOtherThreadSees );
    }

    @Test
    void shouldCountRelationshipsByType()
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode().createRelationshipTo( tx.createNode(), withName( "FOO" ) );
            tx.createNode().createRelationshipTo( tx.createNode(), withName( "FOO" ) );
            tx.createNode().createRelationshipTo( tx.createNode(), withName( "BAR" ) );
            tx.createNode().createRelationshipTo( tx.createNode(), withName( "BAR" ) );
            tx.createNode().createRelationshipTo( tx.createNode(), withName( "BAR" ) );
            tx.createNode().createRelationshipTo( tx.createNode(), withName( "BAZ" ) );
            tx.commit();
        }

        // when
        long total = numberOfRelationships(  );
        long foo = numberOfRelationships( withName( "FOO" ) );
        long bar = numberOfRelationships( withName( "BAR" ) );
        long baz = numberOfRelationships( withName( "BAZ" ) );
        long qux = numberOfRelationships( withName( "QUX" ) );

        // then
        assertEquals( 2, foo );
        assertEquals( 3, bar );
        assertEquals( 1, baz );
        assertEquals( 0, qux );
        assertEquals( 6, total );
    }

    @Test
    void shouldUpdateRelationshipWithLabelCountsWhenDeletingNodeWithRelationship()
    {
        // given
        Node foo;
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.createNode( label( "Foo" ) );
            Node bar = tx.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "BAZ" ) );

            tx.commit();
        }
        long before = numberOfRelationshipsMatching( label( "Foo" ), withName( "BAZ" ), null );

        // when
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.getNodeById( foo.getId() );
            for ( Relationship relationship : foo.getRelationships() )
            {
                relationship.delete();
            }
            foo.delete();

            tx.commit();
        }
        long after = numberOfRelationshipsMatching( label( "Foo" ), withName( "BAZ" ), null );

        // then
        assertEquals( before - 1, after );
    }

    @Test
    void shouldUpdateRelationshipWithLabelCountsWhenDeletingNodesWithRelationships()
    {
        // given
        int numberOfNodes = 2;
        Node[] nodes = new Node[numberOfNodes];
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < numberOfNodes; i++ )
            {
                Node foo = tx.createNode( label( "Foo" + i ) );
                foo.addLabel( Label.label( "Common" ) );
                Node bar = tx.createNode( label( "Bar" + i ) );
                foo.createRelationshipTo( bar, withName( "BAZ" + i ) );
                nodes[i] = foo;
            }

            tx.commit();
        }

        long[] beforeCommon = new long[numberOfNodes];
        long[] before = new long[numberOfNodes];
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            beforeCommon[i] = numberOfRelationshipsMatching( label( "Common" ), withName( "BAZ" + i ), null  );
            before[i] = numberOfRelationshipsMatching( label( "Foo" + i ), withName( "BAZ" + i ), null );
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : nodes )
            {
                node = tx.getNodeById( node.getId() );
                for ( Relationship relationship : node.getRelationships() )
                {
                    relationship.delete();
                }
                node.delete();
            }

            tx.commit();
        }
        long[] afterCommon = new long[numberOfNodes];
        long[] after = new long[numberOfNodes];
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            afterCommon[i] = numberOfRelationshipsMatching( label( "Common" ), withName( "BAZ" + i ), null  );
            after[i] = numberOfRelationshipsMatching( label( "Foo" + i ), withName( "BAZ" + i ), null );
        }

        // then
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            assertEquals( beforeCommon[i] - 1, afterCommon[i] );
            assertEquals( before[i] - 1, after[i] );
        }
    }

    @Test
    void shouldUpdateRelationshipWithLabelCountsWhenRemovingLabelAndDeletingRelationship()
    {
        // given
        Node foo;
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.createNode( label( "Foo" ) );
            Node bar = tx.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "BAZ" ) );

            tx.commit();
        }
        long before = numberOfRelationshipsMatching( label( "Foo" ), withName( "BAZ" ), null );

        // when
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.getNodeById( foo.getId() );
            for ( Relationship relationship : foo.getRelationships() )
            {
                relationship.delete();
            }
            foo.removeLabel( label("Foo"));

            tx.commit();
        }
        long after = numberOfRelationshipsMatching( label( "Foo" ), withName( "BAZ" ), null );

        // then
        assertEquals( before - 1, after );
    }

    private long numberOfRelationships( RelationshipType type )
    {
        return numberOfRelationshipsMatching( null, type, null );
    }

    private long numberOfRelationships()
    {
        return numberOfRelationshipsMatching( null, null, null );
    }

    /** Transactional version of {@link #countsForRelationship(Transaction, Label, RelationshipType, Label)} */
    private long numberOfRelationshipsMatching( Label lhs, RelationshipType type, Label rhs )
    {
        try ( Transaction tx = db.beginTx() )
        {
            long nodeCount = countsForRelationship( tx, lhs, type, rhs );
            tx.commit();
            return nodeCount;
        }
    }

    /**
     * @param start the label of the start node of relationships to get the number of, or {@code null} for "any".
     * @param type  the type of the relationships to get the number of, or {@code null} for "any".
     * @param end   the label of the end node of relationships to get the number of, or {@code null} for "any".
     */
    private long countsForRelationship( Transaction tx, Label start, RelationshipType type, Label end )
    {
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        TokenRead tokenRead = ktx.tokenRead();
        int startId;
        int typeId;
        int endId;
        // start
        if ( start == null )
        {
            startId = TokenRead.ANY_LABEL;
        }
        else
        {
            if ( TokenRead.NO_TOKEN == (startId = tokenRead.nodeLabel( start.name() )) )
            {
                return 0;
            }
        }
        // type
        if ( type == null )
        {
            typeId = TokenRead.ANY_RELATIONSHIP_TYPE;
        }
        else
        {
            if ( TokenRead.NO_TOKEN == (typeId = tokenRead.relationshipType( type.name() )) )
            {
                return 0;
            }
        }
        // end
        if ( end == null )
        {
            endId = TokenRead.ANY_LABEL;
        }
        else
        {
            if ( TokenRead.NO_TOKEN == (endId = tokenRead.nodeLabel( end.name() )) )
            {
                return 0;
            }
        }
        return ktx.dataRead().countsForRelationship( startId, typeId, endId );
    }
}
