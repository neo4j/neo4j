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

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;

@ImpermanentDbmsExtension
class CompositeCountsTest
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldReportNumberOfRelationshipsFromNodesWithGivenLabel()
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            Node foo = tx.createNode( label( "Foo" ) );
            Node fooBar = tx.createNode( label( "Foo" ), label( "Bar" ) );
            Node bar = tx.createNode( label( "Bar" ) );
            foo.createRelationshipTo( tx.createNode(), withName( "ALPHA" ) );
            foo.createRelationshipTo( fooBar, withName( "BETA" ) );
            fooBar.createRelationshipTo( tx.createNode( label( "Bar" ) ), withName( "BETA" ) );
            fooBar.createRelationshipTo( tx.createNode(), withName( "GAMMA" ) );
            bar.createRelationshipTo( tx.createNode( label( "Foo" ) ), withName( "GAMMA" ) );
            tx.commit();
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
    void shouldMaintainCountsOnRelationshipCreate()
    {
        // given
        Node foo;
        Node bar;
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.createNode( label( "Foo" ) );
            bar = tx.createNode( label( "Bar" ) );

            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( foo.getId() ).createRelationshipTo( bar, withName( "KNOWS" ) );

            tx.commit();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
    }

    @Test
    void shouldMaintainCountsOnRelationshipDelete()
    {
        // given
        Relationship relationship;
        try ( Transaction tx = db.beginTx() )
        {
            relationship = tx.createNode( label( "Foo" ) ).createRelationshipTo(
                    tx.createNode( label( "Bar" ) ), withName( "KNOWS" ) );

            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            tx.getRelationshipById( relationship.getId() ).delete();

            tx.commit();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
    }

    @Test
    void shouldMaintainCountsOnLabelAdd()
    {
        // given
        Node foo;
        Node bar;
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.createNode();
            bar = tx.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "KNOWS" ) );

            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( foo.getId() ).addLabel( label( "Foo" ) );

            tx.commit();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
    }

    @Test
    void shouldMaintainCountsOnLabelRemove()
    {
        // given
        Node foo;
        Node bar;
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.createNode( label( "Foo" ) );
            bar = tx.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "KNOWS" ) );

            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( foo.getId() ).removeLabel( label( "Foo" ) );

            tx.commit();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
    }

    @Test
    void shouldMaintainCountsOnLabelAddAndRelationshipCreate()
    {
        // given
        Node foo;
        Node bar;
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.createNode( label( "Foo" ) );
            bar = tx.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "KNOWS" ) );

            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.getNodeById( foo.getId() );
            foo.addLabel( label( "Bar" ) );
            foo.createRelationshipTo( tx.createNode( label( "Foo" ) ), withName( "KNOWS" ) );

            tx.commit();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 2 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 2 );
    }

    @Test
    void shouldMaintainCountsOnLabelRemoveAndRelationshipDelete()
    {
        // given
        Node foo;
        Node bar;
        Relationship rel;
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.createNode( label( "Foo" ), label( "Bar" ) );
            bar = tx.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "KNOWS" ) );
            rel = bar.createRelationshipTo( foo, withName( "KNOWS" ) );

            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( foo.getId() ).removeLabel( label( "Bar" ) );
            tx.getRelationshipById( rel.getId() ).delete();

            tx.commit();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
    }

    @Test
    void shouldMaintainCountsOnLabelAddAndRelationshipDelete()
    {
        // given
        Node foo;
        Node bar;
        Relationship rel;
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.createNode( label( "Foo" ) );
            bar = tx.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "KNOWS" ) );
            rel = bar.createRelationshipTo( foo, withName( "KNOWS" ) );

            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( foo.getId() ).addLabel( label( "Bar" ) );
            tx.getRelationshipById( rel.getId() ).delete();

            tx.commit();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 1 );
    }

    @Test
    void shouldMaintainCountsOnLabelRemoveAndRelationshipCreate()
    {
        // given
        Node foo;
        Node bar;
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.createNode( label( "Foo" ), label( "Bar" ) );
            bar = tx.createNode( label( "Bar" ) );
            foo.createRelationshipTo( bar, withName( "KNOWS" ) );

            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.getNodeById( foo.getId() );
            foo.removeLabel( label( "Bar" ) );
            foo.createRelationshipTo( tx.createNode( label( "Foo" ) ), withName( "KNOWS" ) );

            tx.commit();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 2 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 1 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
    }

    @Test
    void shouldNotUpdateCountsIfCreatedRelationshipIsDeletedInSameTransaction()
    {
        // given
        Node foo;
        Node bar;
        try ( Transaction tx = db.beginTx() )
        {
            foo = tx.createNode( label( "Foo" ) );
            bar = tx.createNode( label( "Bar" ) );

            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( foo.getId() ).createRelationshipTo( bar, withName( "KNOWS" ) ).delete();

            tx.commit();
        }

        // then
        numberOfRelationshipsMatching( label( "Foo" ), withName( "KNOWS" ), null ).shouldBe( 0 );
        numberOfRelationshipsMatching( label( "Bar" ), withName( "KNOWS" ), null ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Foo" ) ).shouldBe( 0 );
        numberOfRelationshipsMatching( null, withName( "KNOWS" ), label( "Bar" ) ).shouldBe( 0 );
    }

    /**
     * Transactional version of {@link #countsForRelationship(Transaction, Label, RelationshipType, Label)}
     */
    private MatchingRelationships numberOfRelationshipsMatching( Label lhs, RelationshipType type, Label rhs )
    {
        try ( Transaction tx = db.beginTx() )
        {
            long nodeCount = countsForRelationship( tx, lhs, type, rhs );
            tx.commit();
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

        MatchingRelationships( String message, long count )
        {
            this.message = message;
            this.count = count;
        }

        void shouldBe( long expected )
        {
            assertEquals( expected, count, message );
        }
    }

    /**
     * @param start the label of the start node of relationships to get the number of, or {@code null} for "any".
     * @param type  the type of the relationships to get the number of, or {@code null} for "any".
     * @param end   the label of the end node of relationships to get the number of, or {@code null} for "any".
     */
    private long countsForRelationship( Transaction tx, Label start, RelationshipType type, Label end )
    {
        KernelTransaction transaction = ((InternalTransaction) tx).kernelTransaction();
        TokenRead tokenRead = transaction.tokenRead();
        int startId;
        int typeId;
        int endId;
        // start
        if ( start == null )
        {
            startId = ANY_LABEL;
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
            typeId = TokenRead.NO_TOKEN;
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
            endId = ANY_LABEL;
        }
        else
        {
            if ( TokenRead.NO_TOKEN == (endId = tokenRead.nodeLabel( end.name() )) )
            {
                return 0;
            }
        }
        return transaction.dataRead().countsForRelationship( startId, typeId, endId );
    }
}
