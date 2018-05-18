/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Optional;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.fulltext.lucene.ScoreEntityIterator;
import org.neo4j.kernel.api.schema.MultiTokenSchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.schema.SchemaDescriptor.ANY_ENTITY_TOKEN;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProviderFactory.DESCRIPTOR;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.multiToken;

public class FulltextIndexProviderTest
{
    private static final String NAME = "fulltext";
    @Rule
    public DatabaseRule db = new EmbeddedDatabaseRule();

    private Node node1;
    private Node node2;

    @Before
    public void prepDB()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            node1 = db.createNode( label( "hej" ), label( "ha" ), label( "he" ) );
            node1.setProperty( "hej", "value" );
            node1.setProperty( "ha", "value1" );
            node1.setProperty( "he", "value2" );
            node1.setProperty( "ho", "value3" );
            node1.setProperty( "hi", "value4" );
            node2 = db.createNode();
            Relationship rel = node1.createRelationshipTo( node2, RelationshipType.withName( "hej" ) );
            rel.setProperty( "hej", "valuuu" );
            rel.setProperty( "ha", "value1" );
            rel.setProperty( "he", "value2" );
            rel.setProperty( "ho", "value3" );
            rel.setProperty( "hi", "value4" );

            transaction.success();
        }
    }

    @Test
    public void createFulltextIndex() throws Exception
    {
        IndexReference fulltextIndex = createIndex( new int[]{7, 8, 9}, new int[]{2, 3, 4} );
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            IndexReference descriptor = transaction.schemaRead().indexGetForName( NAME );
            assertEquals( descriptor.schema(), fulltextIndex.schema() );
            transaction.success();
        }
    }

    @Test
    public void createAndRetainFulltextIndex() throws Exception
    {
        IndexReference fulltextIndex = createIndex( new int[]{7, 8, 9}, new int[]{2, 3, 4} );
        db.restartDatabase( DatabaseRule.RestartAction.EMPTY );

        verifyThatFulltextIndexIsPresent( fulltextIndex );
    }

    @Test
    public void createAndRetainRelationshipFulltextIndex() throws Exception
    {
        IndexReference indexReference;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            MultiTokenSchemaDescriptor schemaDescriptor = multiToken( new int[]{0, 1, 2}, EntityType.RELATIONSHIP, 0, 1, 2, 3 );
            indexReference = transaction.schemaWrite().indexCreate( schemaDescriptor, Optional.of( DESCRIPTOR.name() ), Optional.of( "fulltext" ) );
            transaction.success();
        }
            await( indexReference );
        db.restartDatabase( DatabaseRule.RestartAction.EMPTY );

        verifyThatFulltextIndexIsPresent( indexReference );
    }

    @Test
    public void createAndQueryFulltextIndex() throws Exception
    {
        IndexReference indexReference;
        FulltextIndexProvider provider = (FulltextIndexProvider) db.resolveDependency( IndexProviderMap.class ).lookup( DESCRIPTOR );
        indexReference = createIndex( new int[]{0, 1, 2}, new int[]{0, 1, 2, 3} );
        await( indexReference );
        long thirdNodeid;
        thirdNodeid = createTheThirdNode();
        verifyNodeData( provider, thirdNodeid );
        db.restartDatabase( DatabaseRule.RestartAction.EMPTY );
        provider = (FulltextIndexProvider) db.resolveDependency( IndexProviderMap.class ).lookup( DESCRIPTOR );
        verifyNodeData( provider, thirdNodeid );
    }

    @Test
    public void createAndQueryFulltextRelationshipIndex() throws Exception
    {
        FulltextIndexProvider provider = (FulltextIndexProvider) db.resolveDependency( IndexProviderMap.class ).lookup( DESCRIPTOR );
        IndexReference indexReference;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            MultiTokenSchemaDescriptor schemaDescriptor = multiToken( new int[]{0, 1, 2}, EntityType.RELATIONSHIP, 0, 1, 2, 3 );
            indexReference = transaction.schemaWrite().indexCreate( schemaDescriptor, Optional.of( DESCRIPTOR.name() ), Optional.of( "fulltext" ) );
            transaction.success();
        }
        await( indexReference );
        long secondRelId;
        try ( Transaction transaction = db.beginTx() )
        {
            Relationship ho = node1.createRelationshipTo( node2, RelationshipType.withName( "ho" ) );
            secondRelId = ho.getId();
            ho.setProperty( "hej", "villa" );
            ho.setProperty( "ho", "value3" );
            transaction.success();
        }
        verifyRelationshipData( provider, secondRelId );
        db.restartDatabase( DatabaseRule.RestartAction.EMPTY );
        provider = (FulltextIndexProvider) db.resolveDependency( IndexProviderMap.class ).lookup( DESCRIPTOR );
        verifyRelationshipData( provider, secondRelId );
    }

    @Test
    public void noLabelIsAnyLabel() throws Exception
    {
        IndexReference fulltextIndexDescriptor;
        FulltextIndexProvider provider = (FulltextIndexProvider) db.resolveDependency( IndexProviderMap.class ).lookup( DESCRIPTOR );
        fulltextIndexDescriptor = createIndex( ANY_ENTITY_TOKEN, new int[]{0, 1, 2, 3} );
        await( fulltextIndexDescriptor );
        long thirdNodeId;
        thirdNodeId = createTheThirdNode();
        verifyNodeData( provider, thirdNodeId );
        db.restartDatabase( DatabaseRule.RestartAction.EMPTY );
        provider = (FulltextIndexProvider) db.resolveDependency( IndexProviderMap.class ).lookup( DESCRIPTOR );
        verifyNodeData( provider, thirdNodeId );
    }

    private KernelTransactionImplementation getKernelTransaction()
    {
        try
        {
            return (KernelTransactionImplementation) db.resolveDependency( KernelImpl.class ).newTransaction(
                    org.neo4j.internal.kernel.api.Transaction.Type.explicit, LoginContext.AUTH_DISABLED );
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException("oops");
        }
    }

    private IndexReference createIndex( int[] entityTokens, int[] propertyIds )
            throws TransactionFailureException, InvalidTransactionTypeKernelException, SchemaKernelException

    {
        IndexReference fulltext;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            MultiTokenSchemaDescriptor schemaDescriptor = multiToken( entityTokens, EntityType.NODE, propertyIds );
            fulltext = transaction.schemaWrite().indexCreate( schemaDescriptor, Optional.of( DESCRIPTOR.name() ), Optional.of( NAME ) );
            transaction.success();
        }
        return fulltext;
    }

    private void verifyThatFulltextIndexIsPresent( IndexReference fulltextIndexDescriptor ) throws TransactionFailureException
    {
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            IndexReference descriptor = transaction.schemaRead().indexGetForName( NAME );
            assertEquals( fulltextIndexDescriptor.schema(), descriptor.schema() );
            assertEquals( ((IndexDescriptor) fulltextIndexDescriptor).type(), ((IndexDescriptor) descriptor).type() );
            transaction.success();
        }
    }

    private long createTheThirdNode()
    {
        long secondNodeId;
        try ( Transaction transaction = db.beginTx() )
        {
            Node hej = db.createNode( label( "hej" ) );
            secondNodeId = hej.getId();
            hej.setProperty( "hej", "villa" );
            hej.setProperty( "ho", "value3" );
            transaction.success();
        }
        return secondNodeId;
    }

    private void verifyNodeData( FulltextIndexProvider provider, long thirdNodeid ) throws IndexNotFoundKernelException, ParseException
    {
        try ( Transaction transaction = db.beginTx() )
        {
            ScoreEntityIterator result = provider.query( "fulltext", "value" );
            assertTrue( result.hasNext() );
            assertEquals( 0L, result.next().entityId() );
            assertFalse( result.hasNext() );

            result = provider.query( "fulltext", "villa" );
            assertTrue( result.hasNext() );
            assertEquals( thirdNodeid, result.next().entityId() );
            assertFalse( result.hasNext() );

            result = provider.query( "fulltext", "value3" );
            PrimitiveLongSet ids = Primitive.longSet();
            ids.add( 0L );
            ids.add( thirdNodeid );
            assertTrue( result.hasNext() );
            assertTrue( ids.remove( result.next().entityId() ) );
            assertTrue( result.hasNext() );
            assertTrue( ids.remove( result.next().entityId() ) );
            assertFalse( result.hasNext() );
            transaction.success();
        }
    }

    private void verifyRelationshipData( FulltextIndexProvider provider, long secondRelId ) throws IndexNotFoundKernelException, ParseException
    {
        try ( Transaction transaction = db.beginTx() )
        {
            ScoreEntityIterator result = provider.query( "fulltext", "valuuu" );
            assertTrue( result.hasNext() );
            assertEquals( 0L, result.next().entityId() );
            assertFalse( result.hasNext() );

            result = provider.query( "fulltext", "villa" );
            assertTrue( result.hasNext() );
            assertEquals( secondRelId, result.next().entityId() );
            assertFalse( result.hasNext() );

            result = provider.query( "fulltext", "value3" );
            assertTrue( result.hasNext() );
            assertEquals( 0L, result.next().entityId() );
            assertTrue( result.hasNext() );
            assertEquals( secondRelId, result.next().entityId() );
            assertFalse( result.hasNext() );
            transaction.success();
        }
    }

    private void await( IndexReference descriptor ) throws IndexNotFoundKernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            while ( getKernelTransaction().schemaRead().indexGetState( descriptor ) != InternalIndexState.ONLINE )
            {
                Thread.sleep( 100 );
            }
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
    }
}
