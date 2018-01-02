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
package org.neo4j.kernel.impl.util.dbstructure;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;
import static org.neo4j.kernel.api.ReadOperations.ANY_RELATIONSHIP_TYPE;

public class GraphDbStructureGuideTest
{
    @Test
    public void visitsLabelIds() throws Exception
    {
        // GIVEN
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );
        int personLabelId = createLabel( "Person" );
        int partyLabelId = createLabel( "Party" );
        int animalLabelId = createLabel( "Animal" );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitLabel( personLabelId, "Person" );
        verify( visitor ).visitLabel( partyLabelId, "Party" );
        verify( visitor ).visitLabel( animalLabelId, "Animal" );
    }

    @Test
    public void visitsPropertyKeyIds() throws Exception
    {
        // GIVEN
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );
        int nameId = createPropertyKey( "name" );
        int ageId = createPropertyKey( "age" );
        int osId = createPropertyKey( "os" );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitPropertyKey( nameId, "name" );
        verify( visitor ).visitPropertyKey( ageId, "age" );
        verify( visitor ).visitPropertyKey( osId, "os" );
    }

    @Test
    public void visitsRelationshipTypeIds() throws Exception
    {
        // GIVEN
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );
        int knowsId = createRelTypeId( "KNOWS" );
        int lovesId = createRelTypeId( "LOVES" );
        int fawnsAtId = createRelTypeId( "FAWNS_AT" );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitRelationshipType( knowsId, "KNOWS" );
        verify( visitor ).visitRelationshipType( lovesId, "LOVES" );
        verify( visitor ).visitRelationshipType( fawnsAtId, "FAWNS_AT" );
    }

    @Test
    public void visitsIndexes() throws Exception
    {
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );
        int labelId = createLabel( "Person" );
        int pkId = createPropertyKey( "name" );

        commitAndReOpen();

        IndexDescriptor descriptor = createSchemaIndex( labelId, pkId );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitIndex( descriptor, ":Person(name)", 1.0d, 0l );
    }

    @Test
    public void visitsUniqueConstraintsAndIndices() throws Exception
    {
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );
        int labelId = createLabel( "Person" );
        int pkId = createPropertyKey( "name" );

        commitAndReOpen();

        UniquenessConstraint constraint = createUniqueConstraint( labelId, pkId );
        IndexDescriptor descriptor = new IndexDescriptor( labelId, pkId );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitUniqueIndex( descriptor, ":Person(name)", 1.0d, 0l );
        verify( visitor ).visitUniqueConstraint( constraint, "CONSTRAINT ON ( person:Person ) ASSERT person.name IS " +
                "UNIQUE" );
    }

    @Test
    public void visitsNodeCounts() throws Exception
    {
        // GIVEN
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );
        int personLabelId = createLabeledNodes( "Person", 40 );
        int partyLabelId = createLabeledNodes( "Party", 20 );
        int animalLabelId = createLabeledNodes( "Animal", 30 );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitAllNodesCount( 90 );
        verify( visitor).visitNodeCount( personLabelId, "Person", 40 );
        verify( visitor ).visitNodeCount( partyLabelId, "Party", 20 );
        verify( visitor ).visitNodeCount( animalLabelId, "Animal", 30 );
    }

    @Test
    public void visitsRelCounts() throws Exception
    {
        // GIVEN
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );

        int personLabelId = createLabeledNodes( "Person", 40 );
        int partyLabelId = createLabeledNodes( "Party", 20 );

        int knowsId = createRelTypeId( "KNOWS" );
        int lovesId = createRelTypeId( "LOVES" );

        long personNode = createLabeledNode( personLabelId );
        long partyNode = createLabeledNode( partyLabelId );

        createRel( personNode, knowsId, personNode );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitRelCount( ANY_LABEL, knowsId, ANY_LABEL, "MATCH ()-[:KNOWS]->() RETURN count(*)", 1L );
        verify( visitor ).visitRelCount( ANY_LABEL, lovesId, ANY_LABEL, "MATCH ()-[:LOVES]->() RETURN count(*)", 0L );
        verify( visitor ).visitRelCount( ANY_LABEL, ANY_LABEL, ANY_LABEL, "MATCH ()-[]->() RETURN count(*)", 1L );

        verify( visitor ).visitRelCount( personLabelId, knowsId, ANY_LABEL, "MATCH (:Person)-[:KNOWS]->() RETURN count(*)", 1L );
        verify( visitor ).visitRelCount( ANY_LABEL, knowsId, personLabelId, "MATCH ()-[:KNOWS]->(:Person) RETURN count(*)", 1L );

        verify( visitor ).visitRelCount( personLabelId, lovesId, ANY_LABEL, "MATCH (:Person)-[:LOVES]->() RETURN count(*)", 0L );
        verify( visitor ).visitRelCount( ANY_LABEL, lovesId, personLabelId, "MATCH ()-[:LOVES]->(:Person) RETURN count(*)", 0L );

        verify( visitor ).visitRelCount( personLabelId, ANY_RELATIONSHIP_TYPE, ANY_LABEL, "MATCH (:Person)-[]->() RETURN count(*)", 1L );
        verify( visitor ).visitRelCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, personLabelId, "MATCH ()-[]->(:Person) RETURN count(*)", 1L );

        verify( visitor ).visitRelCount( partyLabelId, knowsId, ANY_LABEL, "MATCH (:Party)-[:KNOWS]->() RETURN count(*)", 0L );
        verify( visitor ).visitRelCount( ANY_LABEL, knowsId, partyLabelId, "MATCH ()-[:KNOWS]->(:Party) RETURN count(*)", 0L );

        verify( visitor ).visitRelCount( partyLabelId, lovesId, ANY_LABEL, "MATCH (:Party)-[:LOVES]->() RETURN count(*)", 0L );
        verify( visitor ).visitRelCount( ANY_LABEL, lovesId, partyLabelId, "MATCH ()-[:LOVES]->(:Party) RETURN count(*)", 0L );

        verify( visitor ).visitRelCount( partyLabelId, ANY_RELATIONSHIP_TYPE, ANY_LABEL, "MATCH (:Party)-[]->() RETURN count(*)", 0L );
        verify( visitor ).visitRelCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, partyLabelId, "MATCH ()-[]->(:Party) RETURN count(*)", 0L );
    }

    private void createRel( long startId, int relTypeId, long endId ) throws Exception
    {
        dataWrite().relationshipCreate( relTypeId, startId, endId );
    }

    private IndexDescriptor createSchemaIndex( int labelId, int pkId ) throws Exception
    {
        return schemaWrite().indexCreate( labelId, pkId );
    }

    private UniquenessConstraint createUniqueConstraint( int labelId, int pkId ) throws Exception
    {
        return schemaWrite().uniquePropertyConstraintCreate( labelId, pkId );
    }

    private int createLabeledNodes( String labelName, int amount ) throws Exception
    {
        int labelId = createLabel( labelName );
        for ( int i = 0; i < amount; i++ )
        {
            createLabeledNode( labelId );
        }
        return labelId;
    }

    private long createLabeledNode( int labelId ) throws Exception
    {
        long nodeId = dataWrite().nodeCreate();
        dataWrite().nodeAddLabel( nodeId, labelId );
        return nodeId;
    }

    private int createLabel( String name ) throws Exception
    {
        return dataWrite().labelGetOrCreateForName( name );
    }

    private int createPropertyKey( String name ) throws Exception
    {
        return dataWrite().propertyKeyGetOrCreateForName( name );
    }

    private int createRelTypeId( String name ) throws Exception
    {
        return dataWrite().relationshipTypeGetOrCreateForName( name );
    }

    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();
    private GraphDatabaseService graph;
    private ThreadToStatementContextBridge bridge;
    private Transaction tx;

    @Before
    public void setUp() throws InvalidTransactionTypeKernelException
    {
        GraphDatabaseAPI api = dbRule.getGraphDatabaseAPI();
        graph = api;
        DependencyResolver dependencyResolver = api.getDependencyResolver();
        this.bridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
        this.tx = graph.beginTx();

    }

    @After
    public void tearDown()
    {
        if ( bridge.hasTransaction() )
        {
            statement().close();
            tx.failure();
            tx.close();
        }
    }

    private Statement statement()
    {
        return bridge.get();
    }

    private ReadOperations read()
    {
        return statement().readOperations();
    }

    private DataWriteOperations dataWrite() throws Exception
    {
        return statement().dataWriteOperations();
    }

    private SchemaWriteOperations schemaWrite() throws Exception
    {
        return statement().schemaWriteOperations();
    }

    public void commitAndReOpen()
    {
        commit();

        tx = graph.beginTx();
    }

    public void accept( DbStructureVisitor visitor )
    {
        commitAndReOpen();

        graph.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
        commit();

        if ( bridge.hasTransaction() )
        {
            throw new IllegalStateException( "Dangling transaction before running visitable" );
        }

        GraphDbStructureGuide analyzer = new GraphDbStructureGuide( graph );
        analyzer.accept( visitor );
    }

    private void commit()
    {
        try
        {
            statement().close();
            tx.success();
        }
        finally
        {
            tx.close();
        }
    }
}
