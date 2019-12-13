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
package org.neo4j.kernel.impl.api.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Race;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.internal.schema.SchemaDescriptor.forRelType;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

@RunWith( Parameterized.class )
public class IndexingServiceIntegrationTest
{
    private static final String FOOD_LABEL = "food";
    private static final String CLOTHES_LABEL = "clothes";
    private static final String WEATHER_LABEL = "weather";
    private static final String PROPERTY_NAME = "name";
    private static final int NUMBER_OF_NODES = 100;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public TestDirectory directory = TestDirectory.testDirectory();
    private GraphDatabaseService database;
    private DatabaseManagementService managementService;

    @Parameterized.Parameters( name = "{0}" )
    public static GraphDatabaseSettings.SchemaIndex[] parameters()
    {
        return GraphDatabaseSettings.SchemaIndex.values();
    }

    @Parameterized.Parameter()
    public GraphDatabaseSettings.SchemaIndex schemaIndex;

    @Before
    public void setUp()
    {
        managementService = new TestDatabaseManagementServiceBuilder( directory.homeDir() ).impermanent()
                .setConfig( GraphDatabaseSettings.default_schema_provider, schemaIndex.providerName() ).build();
        database = managementService.database( DEFAULT_DATABASE_NAME );
        createData( database );
    }

    @After
    public void tearDown()
    {
        try
        {
            managementService.shutdown();
        }
        catch ( Exception e )
        {
            //ignore
        }
    }

    @Test
    public void testManualIndexPopulation() throws InterruptedException, IndexNotFoundKernelException
    {
        IndexDescriptor index;
        try ( Transaction tx = database.beginTx() )
        {
            IndexDefinitionImpl indexDefinition = (IndexDefinitionImpl) tx.schema().indexFor( Label.label( FOOD_LABEL ) ).on( PROPERTY_NAME ).create();
            index = indexDefinition.getIndexReference();
            tx.commit();
        }

        IndexingService indexingService = getIndexingService( database );
        IndexProxy indexProxy = indexingService.getIndexProxy( index );

        waitIndexOnline( indexProxy );
        assertEquals( InternalIndexState.ONLINE, indexProxy.getState() );
        PopulationProgress progress = indexProxy.getIndexPopulationProgress();
        assertEquals( progress.getCompleted(), progress.getTotal() );
    }

    @Test
    public void testManualRelationshipIndexPopulation() throws Exception
    {
        IndexDescriptor index;
        Kernel kernel = ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( Kernel.class );
        try ( KernelTransaction tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED ) )
        {
            int foodId = tx.tokenWrite().relationshipTypeGetOrCreateForName( FOOD_LABEL );
            int propertyId = tx.tokenWrite().propertyKeyGetOrCreateForName( PROPERTY_NAME );
            RelationTypeSchemaDescriptor schema = forRelType( foodId, propertyId );
            index = tx.schemaWrite().indexCreate( schema, "food names" );
            tx.commit();
        }

        IndexingService indexingService = getIndexingService( database );
        IndexProxy indexProxy = indexingService.getIndexProxy( index );

        waitIndexOnline( indexProxy );
        assertEquals( InternalIndexState.ONLINE, indexProxy.getState() );
        PopulationProgress progress = indexProxy.getIndexPopulationProgress();
        assertEquals( progress.getCompleted(), progress.getTotal() );
    }

    @Test
    public void testSchemaIndexMatchIndexingService() throws IndexNotFoundKernelException
    {
        String constraintName = "MyConstraint";
        String indexName = "MyIndex";
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.schema().constraintFor( Label.label( CLOTHES_LABEL ) ).assertPropertyIsUnique( PROPERTY_NAME ).withName( constraintName ).create();
            transaction.schema().indexFor( Label.label( WEATHER_LABEL ) ).on( PROPERTY_NAME ).withName( indexName ).create();

            transaction.commit();
        }

        try ( Transaction tx = database.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }

        IndexingService indexingService = getIndexingService( database );

        IndexProxy clothesIndex = indexingService.getIndexProxy( getIndexByName( constraintName ) );
        IndexProxy weatherIndex = indexingService.getIndexProxy( getIndexByName( indexName ) );
        assertEquals( InternalIndexState.ONLINE, clothesIndex.getState());
        assertEquals( InternalIndexState.ONLINE, weatherIndex.getState());
    }

    @Test
    public void dropIndexDirectlyOnIndexingServiceRaceWithCheckpoint() throws Throwable
    {
        IndexingService indexingService = getIndexingService( database );
        CheckPointer checkPointer = getCheckPointer( database );

        IndexDescriptor indexDescriptor;
        try ( Transaction tx = database.beginTx() )
        {
            IndexDefinitionImpl indexDefinition = (IndexDefinitionImpl) tx.schema().indexFor( Label.label( "label" ) ).on( "prop" ).create();
            indexDescriptor = indexDefinition.getIndexReference();
            tx.commit();
        }
        try ( Transaction tx = database.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.commit();
        }

        Race race = new Race();
        race.addContestant( Race.throwing( () -> checkPointer.forceCheckPoint( new SimpleTriggerInfo( "Test force" ) ) ) );

        race.addContestant( Race.throwing( () -> indexingService.dropIndex( indexDescriptor ) ) );
        race.go();
    }

    @Test
    public void dropIndexRaceWithCheckpoint() throws Throwable
    {
        CheckPointer checkPointer = getCheckPointer( database );

        int nbrOfIndexes = 100;
        try ( Transaction tx = database.beginTx() )
        {
            for ( int i = 0; i < nbrOfIndexes; i++ )
            {
                tx.schema().indexFor( Label.label( "label" ) ).on( "prop" + i ).create();
            }
            tx.commit();
        }
        try ( Transaction tx = database.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.commit();
        }

        AtomicBoolean allIndexesDropped = new AtomicBoolean();
        Race race = new Race();
        race.addContestant( Race.throwing( () -> {
            while ( !allIndexesDropped.get() )
            {
                checkPointer.forceCheckPoint( new SimpleTriggerInfo( "Test force" ) );
            }
        } ) );
        race.addContestant( Race.throwing( () ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                tx.schema().getIndexes().forEach( IndexDefinition::drop );
                tx.commit();
            }
            finally
            {
                allIndexesDropped.set( true );
            }
        } ) );
        race.go();
    }

    private void waitIndexOnline( IndexProxy indexProxy ) throws InterruptedException
    {
        while ( InternalIndexState.ONLINE != indexProxy.getState() )
        {
            Thread.sleep( 10 );
        }
    }

    private IndexingService getIndexingService( GraphDatabaseService database )
    {
        return getDependencyResolver(database).resolveDependency( IndexingService.class );
    }

    private CheckPointer getCheckPointer( GraphDatabaseService database )
    {
        return getDependencyResolver( database ).resolveDependency( CheckPointer.class );
    }

    private DependencyResolver getDependencyResolver( GraphDatabaseService database )
    {
        return ((GraphDatabaseAPI)database).getDependencyResolver();
    }

    private void createData( GraphDatabaseService database )
    {
        for ( int i = 0; i < NUMBER_OF_NODES; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = transaction.createNode( Label.label( FOOD_LABEL ), Label.label( CLOTHES_LABEL ),
                        Label.label( WEATHER_LABEL ) );
                node.setProperty( PROPERTY_NAME, "Node" + i );
                Relationship relationship = node.createRelationshipTo( node, RelationshipType.withName( FOOD_LABEL ) );
                relationship.setProperty( PROPERTY_NAME, "Relationship" + i );
                transaction.commit();
            }
        }
    }

    private IndexDescriptor getIndexByName( String name )
    {
        try ( Transaction tx = database.beginTx() )
        {
            KernelTransaction transaction = ((InternalTransaction) tx).kernelTransaction();
            return transaction.schemaRead().indexGetForName( name );
        }
    }
}
