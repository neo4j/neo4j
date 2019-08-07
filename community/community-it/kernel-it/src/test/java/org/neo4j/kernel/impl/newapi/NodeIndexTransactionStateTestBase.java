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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.impl.collector.Collectors2;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.storable.Values.stringValue;


public abstract class NodeIndexTransactionStateTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringSuffixSearch( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        try ( Transaction tx = beginTransaction() )
        {
            expected.add( nodeWithProp( tx, "1suff" ) );
            nodeWithProp( tx, "pluff" );
            tx.commit();
        }

        createIndex();

        // when
        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "Node" );
            int prop = tx.tokenRead().propertyKey( "prop" );
            expected.add( nodeWithProp( tx, "2suff" ) );
            nodeWithPropId( tx, "skruff" );
            IndexDescriptor index = tx.schemaRead().index( label, prop );
            assertNodeAndValueForSeek( expected, tx, index, needsValues, "pasuff", IndexQuery.stringSuffix( prop, stringValue( "suff" ) ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformScan( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        long nodeToDelete;
        long nodeToChange;
        try ( Transaction tx = beginTransaction() )
        {
            expected.add( nodeWithProp( tx, "suff1" ) );
            expected.add( nodeWithProp( tx, "supp" ) );
            nodeToDelete = nodeWithPropId( tx, "supp" );
            nodeToChange = nodeWithPropId( tx, "supper" );
            tx.commit();
        }

        createIndex();

        // when
        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "Node" );
            int prop = tx.tokenRead().propertyKey( "prop" );
            expected.add( nodeWithProp( tx, "suff2" ) );
            tx.dataWrite().nodeDelete( nodeToDelete );
            tx.dataWrite().nodeRemoveProperty( nodeToChange, prop );

            IndexDescriptor index = tx.schemaRead().index( label, prop );

            // For now, scans cannot request values, since Spatial cannot provide them
            // If we have to possibility to accept values IFF they exist (which corresponds
            // to ValueCapability PARTIAL) this needs to change
            assertNodeAndValueForScan( expected, tx, index, false, "noff" );
        }
    }

    @Test
    void shouldPerformEqualitySeek() throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        try ( Transaction tx = beginTransaction() )
        {
            expected.add( nodeWithProp( tx, "banana" ) );
            nodeWithProp( tx, "apple" );
            tx.commit();
        }

        createIndex();

        // when
        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "Node" );
            int prop = tx.tokenRead().propertyKey( "prop" );
            expected.add( nodeWithProp( tx, "banana" ) );
            nodeWithProp( tx, "dragonfruit" );
            IndexDescriptor index = tx.schemaRead().index( label, prop );
            // Equality seek does never provide values
            assertNodeAndValueForSeek( expected, tx, index, false, "banana", IndexQuery.exact( prop, "banana" ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringPrefixSearch( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        try ( Transaction tx = beginTransaction() )
        {
            expected.add( nodeWithProp( tx, "suff1" ) );
            nodeWithPropId( tx, "supp" );
            tx.commit();
        }

        createIndex();

        // when
        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "Node" );
            int prop = tx.tokenRead().propertyKey( "prop" );
            expected.add( nodeWithProp( tx, "suff2" ) );
            nodeWithPropId( tx, "skruff" );
            IndexDescriptor index = tx.schemaRead().index( label, prop );

            assertNodeAndValueForSeek( expected, tx, index,  needsValues, "suffpa", IndexQuery.stringPrefix( prop, stringValue( "suff" ) ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringRangeSearch( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        try ( Transaction tx = beginTransaction() )
        {
            expected.add( nodeWithProp( tx, "banana" ) );
            nodeWithProp( tx, "apple" );
            tx.commit();
        }

        createIndex();

        // when
        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "Node" );
            int prop = tx.tokenRead().propertyKey( "prop" );
            expected.add( nodeWithProp( tx, "cherry" ) );
            nodeWithProp( tx, "dragonfruit" );
            IndexDescriptor index = tx.schemaRead().index( label, prop );
            assertNodeAndValueForSeek( expected, tx, index, needsValues, "berry", IndexQuery.range( prop, "b", true, "d", false ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringRangeSearchWithAddedNodeInTxState( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        long nodeToChange;
        try ( Transaction tx = beginTransaction() )
        {
            expected.add( nodeWithProp( tx, "banana" ) );
            nodeToChange = nodeWithPropId( tx, "apple" );
            tx.commit();
        }

        createIndex();

        // when
        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "Node" );
            int prop = tx.tokenRead().propertyKey( "prop" );
            expected.add( nodeWithProp( tx, "cherry" ) );
            nodeWithProp( tx, "dragonfruit" );
            IndexDescriptor index = tx.schemaRead().index( label, prop );
            TextValue newProperty = stringValue( "blueberry" );
            tx.dataWrite().nodeSetProperty( nodeToChange, prop, newProperty );
            expected.add(Pair.of(nodeToChange, newProperty ));

            assertNodeAndValueForSeek( expected, tx, index, needsValues, "berry", IndexQuery.range( prop, "b", true, "d", false ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringRangeSearchWithRemovedNodeInTxState( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        long nodeToChange;
        try ( Transaction tx = beginTransaction() )
        {
            nodeToChange = nodeWithPropId( tx, "banana" );
            nodeWithPropId( tx, "apple" );
            tx.commit();
        }

        createIndex();

        // when
        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "Node" );
            int prop = tx.tokenRead().propertyKey( "prop" );
            expected.add( nodeWithProp( tx, "cherry" ) );
            nodeWithProp( tx, "dragonfruit" );
            IndexDescriptor index = tx.schemaRead().index( label, prop );
            TextValue newProperty = stringValue( "kiwi" );
            tx.dataWrite().nodeSetProperty( nodeToChange, prop, newProperty );

            assertNodeAndValueForSeek( expected, tx, index, needsValues, "berry", IndexQuery.range( prop, "b", true, "d", false ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringRangeSearchWithDeletedNodeInTxState( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        long nodeToChange;
        try ( Transaction tx = beginTransaction() )
        {
            nodeToChange = nodeWithPropId( tx, "banana" );
            nodeWithPropId( tx, "apple" );
            tx.commit();
        }

        createIndex();

        // when
        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "Node" );
            int prop = tx.tokenRead().propertyKey( "prop" );
            expected.add( nodeWithProp( tx, "cherry" ) );
            nodeWithProp( tx, "dragonfruit" );
            IndexDescriptor index = tx.schemaRead().index( label, prop );
            tx.dataWrite().nodeDelete( nodeToChange );

            assertNodeAndValueForSeek( expected, tx, index, needsValues, "berry", IndexQuery.range( prop, "b", true, "d", false ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldPerformStringContainsSearch( boolean needsValues ) throws Exception
    {
        // given
        Set<Pair<Long,Value>> expected = new HashSet<>();
        try ( Transaction tx = beginTransaction() )
        {
            expected.add( nodeWithProp( tx, "gnomebat" ) );
            nodeWithPropId( tx, "fishwombat" );
            tx.commit();
        }

        createIndex();

        // when
        try ( Transaction tx = beginTransaction() )
        {
            int label = tx.tokenRead().nodeLabel( "Node" );
            int prop = tx.tokenRead().propertyKey( "prop" );
            expected.add( nodeWithProp( tx, "homeopatic" ) );
            nodeWithPropId( tx, "telephonecompany" );
            IndexDescriptor index = tx.schemaRead().index( label, prop );

            assertNodeAndValueForSeek( expected, tx, index, needsValues, "immense", IndexQuery.stringContains( prop, stringValue( "me" ) ) );
        }
    }

    @Test
    void shouldThrowIfTransactionTerminated() throws Exception
    {
        try ( Transaction tx = beginTransaction() )
        {
            // given
            terminate( tx );

            // when
            assertThrows( TransactionTerminatedException.class, () -> tx.dataRead().nodeExists( 42 ) );
        }
    }

    protected abstract void terminate( Transaction transaction );

    private long nodeWithPropId( Transaction tx, Object value ) throws Exception
    {
        return nodeWithProp( tx, value ).first();
    }

    private Pair<Long,Value> nodeWithProp( Transaction tx, Object value ) throws Exception
    {
        Write write = tx.dataWrite();
        long node = write.nodeCreate();
        write.nodeAddLabel( node, tx.tokenWrite().labelGetOrCreateForName( "Node" ) );
        Value val = Values.of( value );
        write.nodeSetProperty( node, tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" ), val );
        return Pair.of( node, val );
    }

    private void createIndex()
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().indexFor( Label.label( "Node" ) ).on( "prop" ).create();
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }
    }

    /**
     * Perform an index seek and assert that the correct nodes and values were found.
     *
     * Since this method modifies TX state for the test it is not safe to call this method more than once in the same transaction.
     *
     * @param expected the expected nodes and values
     * @param tx the transaction
     * @param index the index
     * @param needsValues if the index is expected to provide values
     * @param anotherValueFoundByQuery a values that would be found by the index queries, if a node with that value existed. This method
     * will create a node with that value, after initializing the cursor and assert that the new node is not found.
     * @param queries the index queries
     */
    private void assertNodeAndValueForSeek( Set<Pair<Long,Value>> expected, Transaction tx, IndexDescriptor index, boolean needsValues,
            Object anotherValueFoundByQuery, IndexQuery... queries ) throws Exception
    {
        try ( NodeValueIndexCursor nodes = tx.cursors().allocateNodeValueIndexCursor() )
        {
            IndexReadSession indexSession = tx.dataRead().indexReadSession( index );
            tx.dataRead().nodeIndexSeek( indexSession, nodes, IndexOrder.NONE, needsValues, queries );
            assertNodeAndValue( expected, tx, needsValues, anotherValueFoundByQuery, nodes );
        }
    }

    /**
     * Perform an index scan and assert that the correct nodes and values were found.
     *
     * Since this method modifies TX state for the test it is not safe to call this method more than once in the same transaction.
     *
     * @param expected the expected nodes and values
     * @param tx the transaction
     * @param index the index
     * @param needsValues if the index is expected to provide values
     * @param anotherValueFoundByQuery a values that would be found by, if a node with that value existed. This method
     * will create a node with that value, after initializing the cursor and assert that the new node is not found.
     */
    private void assertNodeAndValueForScan( Set<Pair<Long,Value>> expected, Transaction tx, IndexDescriptor index, boolean needsValues,
            Object anotherValueFoundByQuery ) throws Exception
    {
        IndexReadSession indexSession = tx.dataRead().indexReadSession( index );
        try ( NodeValueIndexCursor nodes = tx.cursors().allocateNodeValueIndexCursor() )
        {
            tx.dataRead().nodeIndexScan( indexSession, nodes, IndexOrder.NONE, needsValues );
            assertNodeAndValue( expected, tx, needsValues, anotherValueFoundByQuery, nodes );
        }
    }

    private void assertNodeAndValue( Set<Pair<Long,Value>> expected, Transaction tx, boolean needsValues, Object anotherValueFoundByQuery,
            NodeValueIndexCursor nodes ) throws Exception
    {
        // Modify tx state with changes that should not be reflected in the cursor, since it was already initialized in the above statement
        for ( Pair<Long,Value> pair : expected )
        {
            tx.dataWrite().nodeDelete( pair.first() );
        }
        nodeWithPropId( tx, anotherValueFoundByQuery );

        if ( needsValues )
        {
            Set<Pair<Long,Value>> found = new HashSet<>();
            while ( nodes.next() )
            {
                found.add( Pair.of( nodes.nodeReference(), nodes.propertyValue( 0 ) ) );
            }

            assertThat( found, equalTo( expected ) );
        }
        else
        {
            Set<Long> foundIds = new HashSet<>();
            while ( nodes.next() )
            {
                foundIds.add( nodes.nodeReference() );
            }
            ImmutableSet<Long> expectedIds = expected.stream().map( Pair::first ).collect( Collectors2.toImmutableSet() );

            assertThat( foundIds, equalTo( expectedIds ) );
        }
    }
}
