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
package org.neo4j.kernel.counts;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;

import org.neo4j.function.Supplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.Barrier;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.NamedFunction;
import org.neo4j.test.ThreadingRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class RelationshipCountsTest
{
    public final @Rule DatabaseRule db = new ImpermanentDatabaseRule();
    public final @Rule ThreadingRule threading = new ThreadingRule();

    @Test
    public void shouldReportNumberOfRelationshipsInAnEmptyGraph() throws Exception
    {
        // when
        long relationshipCount = numberOfRelationships();

        // then
        assertEquals( 0, relationshipCount );
    }

    @Test
    public void shouldReportTotalNumberOfRelationships() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
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
        assertEquals( 0, during );
        assertEquals( 3, after );
    }

    @Test
    @Ignore("TODO: re-enable this test when we can extract proper counts form TxState")
    public void shouldAccountForDeletedRelationships() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
        Relationship rel;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node = graphDb.createNode();
            node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
            rel = node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
            node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
            tx.success();
        }
        long before = numberOfRelationships(), during;
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
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
        final Barrier.Control barrier = new Barrier.Control();
        long before = numberOfRelationships();
        Future<Long> tx = threading.execute( new NamedFunction<GraphDatabaseService, Long>( "create-relationships" )
        {
            @Override
            public Long apply( GraphDatabaseService graphDb )
            {
                long during;
                try ( Transaction tx = graphDb.beginTx() )
                {
                    Node node = graphDb.createNode();
                    node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
                    node.createRelationshipTo( graphDb.createNode(), withName( "KNOWS" ) );
                    during = countsForRelationship( null, null, null );
                    barrier.reached();
                    tx.success();
                }
                return during;
            }
        }, graphDb );
        barrier.await();

        // when
        long concurrently = numberOfRelationships();
        barrier.release();
        long during = tx.get();
        long after = numberOfRelationships();

        // then
        assertEquals( 0, before );
        assertEquals( 0, concurrently );
        assertEquals( 2, after );
        assertEquals( before, during );
    }

    @Test
    public void shouldNotCountRelationshipsDeletedInOtherTransaction() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
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
                long during;
                try ( Transaction tx = graphDb.beginTx() )
                {
                    rel.delete();
                    during = countsForRelationship( null, null, null );
                    barrier.reached();
                    tx.success();
                }
                return during;
            }
        }, graphDb );
        barrier.await();

        // when
        long concurrently = numberOfRelationships();
        barrier.release();
        long during = tx.get();
        long after = numberOfRelationships();

        // then
        assertEquals( 3, before );
        assertEquals( 3, concurrently );
        assertEquals( 2, after );
        assertEquals( before, during );
    }

    @Test
    public void shouldCountRelationshipsByType() throws Exception
    {
        // given
        final GraphDatabaseService graphDb = db.getGraphDatabaseService();
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
    public void shouldUpdateRelationshipWithLabelCountsWhenDeletingNodeWithRelationship() throws Exception
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
    public void shouldUpdateRelationshipWithLabelCountsWhenRemovingLabelAndDeletingRelationship() throws Exception
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
        try ( Transaction tx = db.getGraphDatabaseService().beginTx() )
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
        ReadOperations read = statementSupplier.get().readOperations();
        int startId, typeId, endId;
        // start
        if ( start == null )
        {
            startId = ReadOperations.ANY_LABEL;
        }
        else
        {
            if ( ReadOperations.NO_SUCH_LABEL == (startId = read.labelGetForName( start.name() )) )
            {
                return 0;
            }
        }
        // type
        if ( type == null )
        {
            typeId = ReadOperations.ANY_RELATIONSHIP_TYPE;
        }
        else
        {
            if ( ReadOperations.NO_SUCH_LABEL == (typeId = read.relationshipTypeGetForName( type.name() )) )
            {
                return 0;
            }
        }
        // end
        if ( end == null )
        {
            endId = ReadOperations.ANY_LABEL;
        }
        else
        {
            if ( ReadOperations.NO_SUCH_LABEL == (endId = read.labelGetForName( end.name() )) )
            {
                return 0;
            }
        }
        return read.countsForRelationship( startId, typeId, endId );
    }

    private Supplier<Statement> statementSupplier;

    @Before
    public void exposeGuts()
    {
        statementSupplier = db.getGraphDatabaseAPI().getDependencyResolver()
                              .resolveDependency( ThreadToStatementContextBridge.class );
    }
}
