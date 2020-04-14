/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.fulltext;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.BeforeEach;

import java.util.Arrays;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;

@DbmsExtension
public class LuceneFulltextTestSupport
{
    static final Label LABEL = Label.label( "LABEL" );
    static final RelationshipType RELTYPE = RelationshipType.withName( "type" );
    static final String PROP = "prop";
    static final String PROP2 = "prop2";
    static final String PROP3 = "prop3";

    @Inject
    DbmsController controller;
    @Inject
    GraphDatabaseAPI db;
    @Inject
    IndexProviderMap indexProviderMap;
    @Inject
    KernelImpl kernel;
    FulltextIndexProvider indexProvider;

    @BeforeEach
    void setUp()
    {
        indexProvider = getAdapter();
    }

    void applySetting( Setting<String> setting, String value )
    {
        controller.restartDbms( builder -> builder.setConfig( setting, value ) );
        indexProvider = getAdapter();
    }

    KernelTransactionImplementation getKernelTransaction()
    {
        try
        {
            return (KernelTransactionImplementation) kernel.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED );
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( "oops" );
        }
    }

    private FulltextIndexProvider getAdapter()
    {
        return (FulltextIndexProvider) indexProviderMap.lookup( FulltextIndexProviderFactory.DESCRIPTOR );
    }

    long createNodeIndexableByPropertyValue( Transaction tx, Label label, Object propertyValue )
    {
        return createNodeWithProperty( tx, label, PROP, propertyValue );
    }

    long createNodeWithProperty( Transaction tx, Label label, String propertyKey, Object propertyValue )
    {
        Node node = tx.createNode( label );
        node.setProperty( propertyKey, propertyValue );
        return node.getId();
    }

    long createRelationshipIndexableByPropertyValue( Transaction transaction, long firstNodeId, long secondNodeId, Object propertyValue )
    {
        return createRelationshipWithProperty( transaction, firstNodeId, secondNodeId, PROP, propertyValue );
    }

    long createRelationshipWithProperty( Transaction transaction, long firstNodeId, long secondNodeId, String propertyKey, Object propertyValue )
    {
        Node first = transaction.getNodeById( firstNodeId );
        Node second = transaction.getNodeById( secondNodeId );
        Relationship relationship = first.createRelationshipTo( second, RELTYPE );
        relationship.setProperty( propertyKey, propertyValue );
        return relationship.getId();
    }

    static KernelTransaction kernelTransaction( Transaction tx )
    {
        assertThat( tx ).isInstanceOf( TransactionImpl.class );
        return ((InternalTransaction)tx).kernelTransaction();
    }

    void assertQueryFindsNothing( KernelTransaction ktx, boolean nodes, String indexName, String query ) throws Exception
    {
        assertQueryFindsIds( ktx, nodes, indexName, query );
    }

    void assertQueryFindsIds( KernelTransaction ktx, boolean nodes, String indexName, String query, long... ids ) throws Exception
    {
        IndexDescriptor index = ktx.schemaRead().indexGetForName( indexName );
        IndexReadSession indexSession = ktx.dataRead().indexReadSession( index );
        MutableLongSet set = LongSets.mutable.of( ids );
        if ( nodes )
        {
            try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor( ktx.pageCursorTracer() ) )
            {
                ktx.dataRead().nodeIndexSeek( indexSession, cursor, unconstrained(), IndexQuery.fulltextSearch( query ) );
                while ( cursor.next() )
                {
                    long nodeId = cursor.nodeReference();
                    assertTrue( set.remove( nodeId ), format( "Result returned node id %d, expected one of %s", nodeId, Arrays.toString( ids ) ) );
                }
            }
        }
        else
        {
            try ( RelationshipIndexCursor cursor = ktx.cursors().allocateRelationshipIndexCursor( ktx.pageCursorTracer() ) )
            {
                ktx.dataRead().relationshipIndexSeek( index, cursor, unconstrained(), IndexQuery.fulltextSearch( query ) );
                while ( cursor.next() )
                {
                    long relationshipId = cursor.relationshipReference();
                    assertTrue( set.remove( relationshipId ), format( "Result returned relationship id %d, expected one of %s",
                            relationshipId, Arrays.toString( ids ) ) );
                }
            }
        }

        if ( !set.isEmpty() )
        {
            fail( "Number of results differ from expected. " + set.size() + " IDs were not found in the result: " + set );
        }
    }

    void assertQueryFindsNodeIdsInOrder( KernelTransaction ktx, String indexName, String query, long... ids )
            throws Exception
    {

        IndexDescriptor index = ktx.schemaRead().indexGetForName( indexName );
        IndexReadSession indexSession = ktx.dataRead().indexReadSession( index );
        try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor( ktx.pageCursorTracer() ) )
        {
            int num = 0;
            float score = Float.MAX_VALUE;
            ktx.dataRead().nodeIndexSeek( indexSession, cursor, unconstrained(), IndexQuery.fulltextSearch( query ) );
            while ( cursor.next() )
            {
                long nextId = cursor.nodeReference();
                float nextScore = cursor.score();
                assertThat( nextScore ).isLessThanOrEqualTo( score );
                score = nextScore;
                assertEquals( ids[num], nextId, format( "Result returned node id %d, expected %d", nextId, ids[num] ) );
                num++;
            }
            assertEquals( ids.length, num, "Number of results differ from expected" );
        }
    }

    void setNodeProp( Transaction transaction, long nodeId, String value )
    {
        setNodeProp( transaction, nodeId, PROP, value );
    }

    void setNodeProp( Transaction transaction, long nodeId, String propertyKey, String value )
    {
        Node node = transaction.getNodeById( nodeId );
        node.setProperty( propertyKey, value );
    }

    void await( IndexDescriptor index ) throws Exception
    {
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            while ( tx.schemaRead().indexGetState( index ) != InternalIndexState.ONLINE )
            {
                Thread.sleep( 100 );
            }
        }
    }
}
