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
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.Supplier;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class CompositeCountsTest
{
    public final @Rule DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldReportNumberOfRelationshipsFromNodesWithGivenLabel() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            Node foo = db.createNode( label( "Foo" ) );
            Node fooBar = db.createNode( label( "Foo" ), label( "Bar" ) );
            Node bar = db.createNode( label( "Bar" ) );
            foo.createRelationshipTo( db.createNode(), withName( "ALPHA" ) );
            foo.createRelationshipTo( fooBar, withName( "BETA" ) );
            fooBar.createRelationshipTo( db.createNode( label( "Bar" ) ), withName( "BETA" ) );
            fooBar.createRelationshipTo( db.createNode(), withName( "GAMMA" ) );
            bar.createRelationshipTo( db.createNode( label( "Foo" ) ), withName( "GAMMA" ) );
            tx.success();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "ALPHA" ), null ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Foo" ), withName( "BETA" ), null ).shouldBe( 2 );
        numberOfRelationshipsMatching( label( "Foo" ), withName( "GAMMA" ), null ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "ALPHA" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "BETA" ), label( "Foo" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "GAMMA" ), label( "Foo" ) ).shouldBe( 1 );

        numberOfRelationshipsMatching( label( "Bar" ), withName( "ALPHA" ), null ).shouldBe( 0 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "BETA" ), null ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "GAMMA" ), null ).shouldBe( 2 );
        numberOfRelationshipsMatching( null, withName( "ALPHA" ), label( "Bar" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "BETA" ), label( "Bar" ) ).shouldBe( 2 );
        numberOfRelationshipsMatching( null, withName( "GAMMA" ), label( "Bar" ) ).shouldBe( 0 );
    }

    @Test
    public void shouldMaintainCountsOnRelationshipCreate() throws Exception
    {
        // given
        Node foo, bar;
        try ( Transaction tx = db.beginTx() )
        {
            foo = db.createNode( label( "Foo" ) );
            bar = db.createNode( label( "Bar" ) );

            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            foo.createRelationshipTo( bar, withName( "KNOWS" ) );

            tx.success();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
    }

    @Test
    public void shouldMaintainCountsOnRelationshipDelete() throws Exception
    {
        // given
        Relationship relationship;
        try ( Transaction tx = db.beginTx() )
        {
            relationship = db.createNode( label( "Foo" ) ).createRelationshipTo(
                    db.createNode( label( "Bar" ) ), withName( "KNOWS" ) );

            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            relationship.delete();

            tx.success();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
    }

    @Test
    public void shouldMaintainCountsOnLabelAdd() throws Exception
    {
        // given
        Node foo, bar;
        try ( Transaction tx = db.beginTx() )
        {
            foo = db.createNode();
            bar = db.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "KNOWS" ) );

            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            foo.addLabel( label( "Foo" ) );

            tx.success();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
    }

    @Test
    public void shouldMaintainCountsOnLabelRemove() throws Exception
    {
        // given
        Node foo, bar;
        try ( Transaction tx = db.beginTx() )
        {
            foo = db.createNode( label( "Foo" ) );
            bar = db.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "KNOWS" ) );

            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            foo.removeLabel( label( "Foo" ) );

            tx.success();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
    }

    @Test
    public void shouldMaintainCountsOnLabelAddAndRelationshipCreate() throws Exception
    {
        // given
        Node foo, bar;
        try ( Transaction tx = db.beginTx() )
        {
            foo = db.createNode( label( "Foo" ) );
            bar = db.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "KNOWS" ) );

            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            foo.addLabel( label( "Bar" ) );
            foo.createRelationshipTo( db.createNode( label( "Foo" ) ), withName( "KNOWS" ) );

            tx.success();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 2 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 2 );
    }

    @Test
    public void shouldMaintainCountsOnLabelRemoveAndRelationshipDelete() throws Exception
    {
        // given
        Node foo, bar;
        Relationship rel;
        try ( Transaction tx = db.beginTx() )
        {
            foo = db.createNode( label( "Foo" ), label( "Bar" ) );
            bar = db.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "KNOWS" ) );
            rel = bar.createRelationshipTo( foo, withName( "KNOWS" ) );

            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            foo.removeLabel( label( "Bar" ) );
            rel.delete();

            tx.success();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
    }

    @Test
    public void shouldMaintainCountsOnLabelAddAndRelationshipDelete() throws Exception
    {
        // given
        Node foo, bar;
        Relationship rel;
        try ( Transaction tx = db.beginTx() )
        {
            foo = db.createNode( label( "Foo" ) );
            bar = db.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "KNOWS" ) );
            rel = bar.createRelationshipTo( foo, withName( "KNOWS" ) );

            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            foo.addLabel( label( "Bar" ) );
            rel.delete();

            tx.success();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 1 );
    }

    @Test
    public void shouldMaintainCountsOnLabelRemoveAndRelationshipCreate() throws Exception
    {
        // given
        Node foo, bar;
        try ( Transaction tx = db.beginTx() )
        {
            foo = db.createNode( label( "Foo" ), label( "Bar" ) );
            bar = db.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "KNOWS" ) );

            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            foo.removeLabel( label( "Bar" ) );
            foo.createRelationshipTo( db.createNode( label( "Foo" ) ), withName( "KNOWS" ) );

            tx.success();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 2 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
    }

    @Test
    public void shouldNotUpdateCountsIfCreatedRelationshipIsDeletedInSameTransaction() throws Exception
    {
        // given
        Node foo, bar;
        try ( Transaction tx = db.beginTx() )
        {
            foo = db.createNode( label( "Foo" ) );
            bar = db.createNode( label( "Bar" ) );

            tx.success();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            foo.createRelationshipTo( bar, withName( "KNOWS" ) ).delete();

            tx.success();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 0 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 0 );
    }

    /**
     * Transactional version of {@link #countsForRelationship(Label, RelationshipType, Label)}
     */
    private MatchingRelationships numberOfRelationshipsMatching( Label lhs, RelationshipType type, Label rhs )
    {
        try ( Transaction tx = db.getGraphDatabaseService().beginTx() )
        {
            long nodeCount = countsForRelationship( lhs, type, rhs );
            tx.success();
            return new MatchingRelationships( String.format( "(%s)-%s->(%s)",
                                                             lhs == null ? "" : ":" + lhs.name(),
                                                             type == null ? "" : "[:" + type.name() + "]",
                                                             rhs == null ? "" : ":" + rhs.name() ), nodeCount );
        }
    }

    private static class MatchingRelationships
    {
        private final String message;
        private final long count;

        public MatchingRelationships( String message, long count )
        {
            this.message = message;
            this.count = count;
        }

        public void shouldBe( long expected )
        {
            assertEquals( message, expected, count );
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
