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
package org.neo4j.kernel.api.impl.fulltext.integrations.kernel;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInCompositeSchemaException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.impl.fulltext.lucene.ScoreEntityIterator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.extension.dependency.AllByPrioritySelectionStrategy;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;

public class FulltextIndexProviderTest
{
    @Rule
    public DatabaseRule db = new EmbeddedDatabaseRule();
    private static final String STANDARD = StandardAnalyzer.class.getCanonicalName();

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
    public void shouldProvideFulltextIndexProviderForFulltextIndexDescriptor()
    {
        AllByPrioritySelectionStrategy<IndexProvider<?>> indexProviderSelection = new AllByPrioritySelectionStrategy<>();
        IndexProvider defaultIndexProvider = db.getDependencyResolver().resolveDependency( IndexProvider.class, indexProviderSelection );

        IndexProviderMap indexProviderMap = new DefaultIndexProviderMap( defaultIndexProvider, indexProviderSelection.lowerPrioritizedCandidates() );
        IndexProvider provider = indexProviderMap.apply( FulltextIndexProviderFactory.DESCRIPTOR );
        IndexDescriptor fulltextIndexDescriptor;
        try ( Transaction ignore = db.beginTx() )
        {
            fulltextIndexDescriptor =
                    provider.indexDescriptorFor( SchemaDescriptorFactory.multiToken( new int[0], EntityType.NODE, new int[]{2, 3, 4} ), "fulltext", STANDARD );
            assertThat( fulltextIndexDescriptor, is( instanceOf( FulltextIndexDescriptor.class ) ) );
        }
        assertThat( indexProviderMap.getProviderFor( fulltextIndexDescriptor ), is( instanceOf( FulltextIndexProvider.class ) ) );
    }

    @Test
    public void createFulltextIndex() throws Exception
    {
        IndexDescriptor fulltextIndexDescriptor;
        IndexProvider provider = db.resolveDependency( IndexProviderMap.class ).apply( FulltextIndexProviderFactory.DESCRIPTOR );
        fulltextIndexDescriptor = createIndex( provider, new int[]{7, 8, 9}, new int[]{2, 3, 4} );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            IndexDescriptor descriptor = stmt.readOperations().indexGetForSchema( fulltextIndexDescriptor.schema() );
            assertEquals( descriptor.schema(), fulltextIndexDescriptor.schema() );
            transaction.success();
        }
    }

    @Test
    public void createAndRetainFulltextIndex() throws Exception
    {
        IndexDescriptor fulltextIndexDescriptor;
        IndexProvider provider = db.resolveDependency( IndexProviderMap.class ).apply( FulltextIndexProviderFactory.DESCRIPTOR );
        fulltextIndexDescriptor = createIndex( provider, new int[]{7, 8, 9}, new int[]{2, 3, 4} );
        db.restartDatabase( DatabaseRule.RestartAction.EMPTY );

        verifyThatFulltextIndexIsPresent( fulltextIndexDescriptor );
    }

    @Test
    public void createAndRetainRelationshipFulltextIndex() throws Exception
    {
        IndexDescriptor fulltextIndexDescriptor;
        IndexProvider provider = db.resolveDependency( IndexProviderMap.class ).apply( FulltextIndexProviderFactory.DESCRIPTOR );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            fulltextIndexDescriptor =
                    provider.indexDescriptorFor( SchemaDescriptorFactory.multiToken( new int[]{7, 8, 9}, EntityType.RELATIONSHIP, new int[]{2, 3, 4} ), "rels",
                            STANDARD );
            stmt.schemaWriteOperations().nonSchemaIndexCreate( fulltextIndexDescriptor );
            transaction.success();
        }
        db.restartDatabase( DatabaseRule.RestartAction.EMPTY );

        verifyThatFulltextIndexIsPresent( fulltextIndexDescriptor );
    }

    @Test
    public void createAndQueryFulltextIndex() throws Exception
    {
        IndexDescriptor fulltextIndexDescriptor;
        FulltextIndexProvider provider =
                (FulltextIndexProvider) db.resolveDependency( IndexProviderMap.class ).apply( FulltextIndexProviderFactory.DESCRIPTOR );
        fulltextIndexDescriptor = createIndex( provider, new int[]{0, 1, 2}, new int[]{0, 1, 2, 3} );
        await( fulltextIndexDescriptor );
        long thirdNodeid;
        thirdNodeid = createTheThirdNode();
        verifyNodeData( fulltextIndexDescriptor, provider, thirdNodeid );
        db.restartDatabase( DatabaseRule.RestartAction.EMPTY );
        provider = (FulltextIndexProvider) db.resolveDependency( IndexProviderMap.class ).apply( FulltextIndexProviderFactory.DESCRIPTOR );
        verifyNodeData( fulltextIndexDescriptor, provider, thirdNodeid );
    }

    @Test
    public void createAndQueryFulltextRelationshipIndex() throws Exception
    {
        IndexDescriptor fulltextIndexDescriptor;
        FulltextIndexProvider provider =
                (FulltextIndexProvider) db.resolveDependency( IndexProviderMap.class ).apply( FulltextIndexProviderFactory.DESCRIPTOR );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            fulltextIndexDescriptor =
                    provider.indexDescriptorFor( SchemaDescriptorFactory.multiToken( new int[]{0, 1, 2}, EntityType.RELATIONSHIP, new int[]{0, 1, 2, 3} ),
                            "fulltext", STANDARD );
            stmt.schemaWriteOperations().nonSchemaIndexCreate( fulltextIndexDescriptor );
            transaction.success();
        }
        await( fulltextIndexDescriptor );
        long secondRelId;
        try ( Transaction transaction = db.beginTx() )
        {
            Relationship ho = node1.createRelationshipTo( node2, RelationshipType.withName( "ho" ) );
            secondRelId = ho.getId();
            ho.setProperty( "hej", "villa" );
            ho.setProperty( "ho", "value3" );
            transaction.success();
        }
        verifyRelationshipData( fulltextIndexDescriptor, provider, secondRelId );
        db.restartDatabase( DatabaseRule.RestartAction.EMPTY );
        provider = (FulltextIndexProvider) db.resolveDependency( IndexProviderMap.class ).apply( FulltextIndexProviderFactory.DESCRIPTOR );
        verifyRelationshipData( fulltextIndexDescriptor, provider, secondRelId );
    }

    @Test
    public void noLabelIsAnyLabel() throws Exception
    {
        IndexDescriptor fulltextIndexDescriptor;
        FulltextIndexProvider provider =
                (FulltextIndexProvider) db.resolveDependency( IndexProviderMap.class ).apply( FulltextIndexProviderFactory.DESCRIPTOR );
        fulltextIndexDescriptor = createIndex( provider, new int[0], new int[]{0, 1, 2, 3} );
        await( fulltextIndexDescriptor );
        long thirdNodeId;
        thirdNodeId = createTheThirdNode();
        verifyNodeData( fulltextIndexDescriptor, provider, thirdNodeId );
        db.restartDatabase( DatabaseRule.RestartAction.EMPTY );
        provider = (FulltextIndexProvider) db.resolveDependency( IndexProviderMap.class ).apply( FulltextIndexProviderFactory.DESCRIPTOR );
        verifyNodeData( fulltextIndexDescriptor, provider, thirdNodeId );
    }

    private IndexDescriptor createIndex( IndexProvider provider, int[] entityTokens, int[] propertyIds )
            throws AlreadyConstrainedException, AlreadyIndexedException, RepeatedPropertyInCompositeSchemaException, InvalidTransactionTypeKernelException
    {
        IndexDescriptor fulltextIndexDescriptor;
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            fulltextIndexDescriptor =
                    provider.indexDescriptorFor( SchemaDescriptorFactory.multiToken( entityTokens, EntityType.NODE, propertyIds ), "fulltext",
                            STANDARD );
            stmt.schemaWriteOperations().nonSchemaIndexCreate( fulltextIndexDescriptor );
            transaction.success();
        }
        return fulltextIndexDescriptor;
    }

    private void verifyThatFulltextIndexIsPresent( IndexDescriptor fulltextIndexDescriptor ) throws SchemaRuleNotFoundException
    {
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            IndexDescriptor descriptor = stmt.readOperations().indexGetForSchema( fulltextIndexDescriptor.schema() );
            assertThat( fulltextIndexDescriptor, is( instanceOf( FulltextIndexDescriptor.class ) ) );
            assertEquals( fulltextIndexDescriptor.schema(), descriptor.schema() );
            assertEquals( fulltextIndexDescriptor.type(), descriptor.type() );
            assertEquals( fulltextIndexDescriptor.identifier(), descriptor.identifier() );
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

    private void verifyNodeData( IndexDescriptor fulltextIndexDescriptor, FulltextIndexProvider provider, long thircNodeid ) throws IOException
    {
        try ( Transaction transaction = db.beginTx() )
        {
            ScoreEntityIterator result = provider.query( fulltextIndexDescriptor, "value" );
            assertTrue( result.hasNext() );
            assertEquals( 0L, result.next().entityId() );
            assertFalse( result.hasNext() );

            result = provider.query( fulltextIndexDescriptor, "villa" );
            assertTrue( result.hasNext() );
            assertEquals( thircNodeid, result.next().entityId() );
            assertFalse( result.hasNext() );

            result = provider.query( fulltextIndexDescriptor, "value3" );
            assertTrue( result.hasNext() );
            assertEquals( 0L, result.next().entityId() );
            assertTrue( result.hasNext() );
            assertEquals( thircNodeid, result.next().entityId() );
            assertFalse( result.hasNext() );
            transaction.success();
        }
    }

    private void verifyRelationshipData( IndexDescriptor fulltextIndexDescriptor, FulltextIndexProvider provider, long secondRelId ) throws IOException
    {
        try ( Transaction transaction = db.beginTx() )
        {
            ScoreEntityIterator result = provider.query( fulltextIndexDescriptor, "valuuu" );
            assertTrue( result.hasNext() );
            assertEquals( 0L, result.next().entityId() );
            assertFalse( result.hasNext() );

            result = provider.query( fulltextIndexDescriptor, "villa" );
            assertTrue( result.hasNext() );
            assertEquals( secondRelId, result.next().entityId() );
            assertFalse( result.hasNext() );

            result = provider.query( fulltextIndexDescriptor, "value3" );
            assertTrue( result.hasNext() );
            assertEquals( 0L, result.next().entityId() );
            assertTrue( result.hasNext() );
            assertEquals( secondRelId, result.next().entityId() );
            assertFalse( result.hasNext() );
            transaction.success();
        }
    }

    private void await( IndexDescriptor fulltextIndexDescriptor ) throws IndexNotFoundKernelException
    {
        try ( Transaction ignore = db.beginTx(); Statement stmt = db.statement() )
        {
            while ( stmt.readOperations().indexGetState( fulltextIndexDescriptor ) != InternalIndexState.ONLINE )
            {
                continue;
            }
        }
    }
}
