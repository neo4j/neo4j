/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Provider;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.counts.keys.RelationshipKey;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.kernel.impl.store.CountsComputer.computeCounts;

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
        verifyAllCounts();
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
        verifyAllCounts();

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
        verifyAllCounts();
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
        verifyAllCounts();
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
        verifyAllCounts();

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
        verifyAllCounts();
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
        verifyAllCounts();
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
        verifyAllCounts();
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
        verifyAllCounts();
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
        verifyAllCounts();
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
        verifyAllCounts();
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
        verifyAllCounts();
    }

    @Test
    public void shouldAccessCountsInTransaction() throws Exception
    {
        // given
        Label label = label( "Foo" );
        RelationshipType type = withName( "BAR" );
        
        Node delN;
        Relationship delR;
        try ( Transaction tx = db.beginTx() )
        {
            delN = db.createNode( label );
            Node a = db.createNode( label );
            Node b = db.createNode( label );
            delR = a.createRelationshipTo( b, type );
            a.createRelationshipTo( db.createNode(), type );
            b.createRelationshipTo( db.createNode(), type );
            delN.createRelationshipTo( db.createNode(  ), type );
            delN.createRelationshipTo( db.createNode( label ), type );

            tx.success();
        }

        // when
        long out, in;
        try ( Transaction tx = db.beginTx() )
        {
            for ( Relationship rel : delN.getRelationships() )
            {
                rel.delete();
            }
            delN.delete();
            delR.delete();

            out = countsForRelationship( label, type, null );
            in = countsForRelationship( null, type, label );

            tx.success();
        }

        // then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( countsForRelationship( label, type, null ), out );
            assertEquals( countsForRelationship( null, type, label ), in );
        }
    }

    private void verifyAllCounts()
    {
        NeoStore stores = db.resolveDependency( NeoStoreProvider.class ).evaluate();
        List<CountsRecordState.Difference> differences = computeCounts( stores ).verify( stores.getCounts() );
        if ( !differences.isEmpty() )
        {
            StringBuilder error = new StringBuilder();
            for ( CountsRecordState.Difference difference : differences )
            {
                if ( difference.key() instanceof RelationshipKey )
                {
                    RelationshipKey key = (RelationshipKey) difference.key();
                    if ( key.startLabelId() != ReadOperations.ANY_LABEL &&
                         key.endLabelId() != ReadOperations.ANY_LABEL )
                    {
                        continue;
                    }
                }
                error.append( "\n\t" ).append( difference );
            }
            if ( error.length() > 0 )
            {
                fail( error.toString() );
            }
        }
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
        ReadOperations read = statementProvider.instance().readOperations();
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

    private Provider<Statement> statementProvider;

    @Before
    public void exposeGuts()
    {
        statementProvider = db.getGraphDatabaseAPI().getDependencyResolver()
                              .resolveDependency( ThreadToStatementContextBridge.class );
    }
}
