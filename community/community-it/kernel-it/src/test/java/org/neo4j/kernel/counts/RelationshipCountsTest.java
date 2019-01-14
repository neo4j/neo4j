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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.Barrier;
import org.neo4j.test.NamedFunction;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

public class RelationshipCountsTest
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();
    @Rule
    public final ThreadingRule threading = new ThreadingRule();
    private Supplier<KernelTransaction> ktxSupplier;

    @Before
    public void exposeGuts()
    {
        ktxSupplier = () -> db.getGraphDatabaseAPI().getDependencyResolver()
                .resolveDependency( ThreadToStatementContextBridge.class )
                .getKernelTransactionBoundToThisThread( true );
    }

    @Test
    public void shouldReportNumberOfRelationshipsInAnEmptyGraph()
    {
        // when
        long relationshipCount = numberOfRelationships();

        // then
        assertEquals( 0, relationshipCount );
    }

    @Test
    public void shouldReportTotalNumberOfRelationships()
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
        long before = numberOfRelationships();
        long during;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node = graphDb.createNode();
            node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
            node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
            node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
            during = countsForRelationship( null, null, null );
            tx.success();
        }

        // when
        long after = numberOfRelationships();

        // then
        assertEquals( 0, before );
        assertEquals( 3, during );
        assertEquals( 3, after );
    }

    @Test
    public void shouldAccountForDeletedRelationships()
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
        Relationship rel;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node = graphDb.createNode();
            node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
            rel = node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
            node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
            tx.success();
        }
        long before = numberOfRelationships();
        long during;
        try ( Transaction tx = graphDb.beginTx() )
        {
            rel.delete();
            during = countsForRelationship( null, null, null );
            tx.success();
        }

        // when
        long after = numberOfRelationships();

        // then
        assertEquals( 3, before );
        assertEquals( 2, during );
        assertEquals( 2, after );
    }

    @Test
    public void shouldNotCountRelationshipsCreatedInOtherTransaction() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
        final Barrier.Control barrier = new Barrier.Control();
        long before = numberOfRelationships();
        Future<Long> tx = threading.execute( new NamedFunction<GraphDatabaseService, Long>( "create-relationships" )
        {
            @Override
            public Long apply( GraphDatabaseService graphDb )
            {
                try ( Transaction tx = graphDb.beginTx() )
                {
                    Node node = graphDb.createNode();
                    node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
                    node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
                    long whatThisThreadSees = countsForRelationship( null, null, null );
                    barrier.reached();
                    tx.success();
                    return whatThisThreadSees;
                }
            }
        }, graphDb );
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
    public void shouldNotCountRelationshipsDeletedInOtherTransaction() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
        final Relationship rel;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node = graphDb.createNode();
            node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
            rel = node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
            node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
            tx.success();
        }
        final Barrier.Control barrier = new Barrier.Control();
        long before = numberOfRelationships();
        Future<Long> tx = threading.execute( new NamedFunction<GraphDatabaseService, Long>( "create-relationships" )
        {
            @Override
            public Long apply( GraphDatabaseService graphDb )
            {
                try ( Transaction tx = graphDb.beginTx() )
                {
                    rel.delete();
                    long whatThisThreadSees = countsForRelationship( null, null, null );
                    barrier.reached();
                    tx.success();
                    return whatThisThreadSees;
                }
            }
        }, graphDb );
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
    public void shouldCountRelationshipsByType()
    {
        // given
        final GraphDatabaseService graphDb = db.getGraphDatabaseAPI();
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.createNode().createRelationshipTo( graphDb.createNode(), withName( "FOO" ) );
            graphDb.createNode().createRelationshipTo( graphDb.createNode(), withName( "FOO" ) );
            graphDb.createNode().createRelationshipTo( graphDb.createNode(), withName( "BAR" ) );
            graphDb.createNode().createRelationshipTo( graphDb.createNode(), withName( "BAR" ) );
            graphDb.createNode().createRelationshipTo( graphDb.createNode(), withName( "BAR" ) );
            graphDb.createNode().createRelationshipTo( graphDb.createNode(), withName( "BAZ" ) );
            tx.success();
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
    public void shouldUpdateRelationshipWithLabelCountsWhenDeletingNodeWithRelationship()
    {
        // given
        Node foo;
        try ( Transaction tx = db.beginTx() )
        {
            foo = db.createNode( label( "Foo" ) );
            Node bar = db.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "BAZ" ) );

            tx.success();
        }
        long before = numberOfRelationshipsMatching( label( "Foo" ), withName( "BAZ" ), null );

        // when
        try ( Transaction tx = db.beginTx() )
        {
            for ( Relationship relationship : foo.getRelationships() )
            {
                relationship.delete();
            }
            foo.delete();

            tx.success();
        }
        long after = numberOfRelationshipsMatching( label( "Foo" ), withName( "BAZ" ), null );

        // then
        assertEquals( before - 1, after );
    }

    @Test
    public void shouldUpdateRelationshipWithLabelCountsWhenDeletingNodesWithRelationships()
    {
        // given
        int numberOfNodes = 2;
        Node[] nodes = new Node[numberOfNodes];
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < numberOfNodes; i++ )
            {
                Node foo = db.createNode( label( "Foo" + i ) );
                foo.addLabel( Label.label( "Common" ) );
                Node bar = db.createNode( label( "Bar" + i ) );
                foo.createRelationshipTo( bar, withName( "BAZ" + i ) );
                nodes[i] = foo;
            }

            tx.success();
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
                for ( Relationship relationship : node.getRelationships() )
                {
                    relationship.delete();
                }
                node.delete();
            }

            tx.success();
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
    public void shouldUpdateRelationshipWithLabelCountsWhenRemovingLabelAndDeletingRelationship()
    {
        // given
        Node foo;
        try ( Transaction tx = db.beginTx() )
        {
            foo = db.createNode( label( "Foo" ) );
            Node bar = db.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "BAZ" ) );

            tx.success();
        }
        long before = numberOfRelationshipsMatching( label( "Foo" ), withName( "BAZ" ), null );

        // when
        try ( Transaction tx = db.beginTx() )
        {
            for ( Relationship relationship : foo.getRelationships() )
            {
                relationship.delete();
            }
            foo.removeLabel( label("Foo"));

            tx.success();
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

    /** Transactional version of {@link #countsForRelationship(Label, RelationshipType, Label)} */
    private long numberOfRelationshipsMatching( Label lhs, RelationshipType type, Label rhs )
    {
        try ( Transaction tx = db.getGraphDatabaseAPI().beginTx() )
        {
            long nodeCount = countsForRelationship( lhs, type, rhs );
            tx.success();
            return nodeCount;
        }
    }

    /**
     * @param start the label of the start node of relationships to get the number of, or {@code null} for "any".
     * @param type  the type of the relationships to get the number of, or {@code null} for "any".
     * @param end   the label of the end node of relationships to get the number of, or {@code null} for "any".
     */
    private long countsForRelationship( Label start, RelationshipType type, Label end )
    {
        KernelTransaction ktx = ktxSupplier.get();
        try ( Statement ignore = ktx.acquireStatement() )
        {
            TokenRead tokenRead = ktx.tokenRead();
            int startId;
            int typeId;
            int endId;
            // start
            if ( start == null )
            {
                startId = Read.ANY_LABEL;
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
                typeId = Read.ANY_RELATIONSHIP_TYPE;
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
                endId = Read.ANY_LABEL;
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
}
