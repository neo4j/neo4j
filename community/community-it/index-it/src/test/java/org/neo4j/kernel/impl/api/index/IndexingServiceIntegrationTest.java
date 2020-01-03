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
package org.neo4j.kernel.impl.api.index;

import org.eclipse.collections.api.iterator.LongIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.test.Race;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forRelType;

@RunWith( Parameterized.class )
public class IndexingServiceIntegrationTest
{
    private static final String FOOD_LABEL = "food";
    private static final String CLOTHES_LABEL = "clothes";
    private static final String WEATHER_LABEL = "weather";
    private static final String PROPERTY_NAME = "name";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public TestDirectory directory = TestDirectory.testDirectory();
    private GraphDatabaseService database;

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
        database = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( directory.storeDir() )
                .setConfig( GraphDatabaseSettings.default_schema_provider, schemaIndex.providerName() )
                .newGraphDatabase();
        createData( database, 100 );
    }

    @After
    public void tearDown()
    {
        try
        {
            database.shutdown();
        }
        catch ( Exception e )
        {
            //ignore
        }
    }

    @Test
    public void testManualIndexPopulation() throws InterruptedException, IndexNotFoundKernelException
    {
        try ( Transaction tx = database.beginTx() )
        {
            database.schema().indexFor( Label.label( FOOD_LABEL ) ).on( PROPERTY_NAME ).create();
            tx.success();
        }

        int labelId = getLabelId( FOOD_LABEL );
        int propertyKeyId = getPropertyKeyId( PROPERTY_NAME );

        IndexingService indexingService = getIndexingService( database );
        IndexProxy indexProxy = indexingService.getIndexProxy( forLabel( labelId, propertyKeyId ) );

        waitIndexOnline( indexProxy );
        assertEquals( InternalIndexState.ONLINE, indexProxy.getState() );
        PopulationProgress progress = indexProxy.getIndexPopulationProgress();
        assertEquals( progress.getCompleted(), progress.getTotal() );
    }

    @Test
    public void testManualRelationshipIndexPopulation() throws Exception
    {
        RelationTypeSchemaDescriptor descriptor;
        try ( org.neo4j.internal.kernel.api.Transaction tx =
                ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( Kernel.class ).beginTransaction( explicit, AUTH_DISABLED ) )
        {
            int foodId = tx.tokenWrite().relationshipTypeGetOrCreateForName( FOOD_LABEL );
            int propertyId = tx.tokenWrite().propertyKeyGetOrCreateForName( PROPERTY_NAME );
            descriptor = forRelType( foodId, propertyId );
            tx.schemaWrite().indexCreate( descriptor );
            tx.success();
        }

        IndexingService indexingService = getIndexingService( database );
        IndexProxy indexProxy = indexingService.getIndexProxy( descriptor );

        waitIndexOnline( indexProxy );
        assertEquals( InternalIndexState.ONLINE, indexProxy.getState() );
        PopulationProgress progress = indexProxy.getIndexPopulationProgress();
        assertEquals( progress.getCompleted(), progress.getTotal() );
    }

    @Test
    public void testSchemaIndexMatchIndexingService() throws IndexNotFoundKernelException
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().constraintFor( Label.label( CLOTHES_LABEL ) ).assertPropertyIsUnique( PROPERTY_NAME ).create();
            database.schema().indexFor( Label.label( WEATHER_LABEL ) ).on( PROPERTY_NAME ).create();

            transaction.success();
        }

        try ( Transaction ignored = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }

        IndexingService indexingService = getIndexingService( database );
        int clothedLabelId = getLabelId( CLOTHES_LABEL );
        int weatherLabelId = getLabelId( WEATHER_LABEL );
        int propertyId = getPropertyKeyId( PROPERTY_NAME );

        IndexProxy clothesIndex = indexingService.getIndexProxy( forLabel( clothedLabelId, propertyId ) );
        IndexProxy weatherIndex = indexingService.getIndexProxy( forLabel( weatherLabelId, propertyId ) );
        assertEquals( InternalIndexState.ONLINE, clothesIndex.getState());
        assertEquals( InternalIndexState.ONLINE, weatherIndex.getState());
    }

    @Test
    public void dropIndexDirectlyOnIndexingServiceRaceWithCheckpoint() throws Throwable
    {
        IndexingService indexingService = getIndexingService( database );
        CheckPointer checkPointer = getCheckPointer( database );

        try ( Transaction tx = database.beginTx() )
        {
            database.schema().indexFor( Label.label( "label" ) ).on( "prop" ).create();
            tx.success();
        }
        try ( Transaction tx = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.success();
        }

        Race race = new Race();
        race.addContestant( Race.throwing( () -> checkPointer.forceCheckPoint( new SimpleTriggerInfo( "Test force" ) ) ) );

        long indexId = oneIndex( indexingService );
        CapableIndexDescriptor indexDescriptor = asCapableIndexDescriptor( indexingService, indexId );
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
                database.schema().indexFor( Label.label( "label" ) ).on( "prop" + i ).create();
            }
            tx.success();
        }
        try ( Transaction tx = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.success();
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
                database.schema().getIndexes().forEach( IndexDefinition::drop );
                tx.success();
            }
            finally
            {
                allIndexesDropped.set( true );
            }
        } ) );
        race.go();
    }

    private long oneIndex( IndexingService indexingService )
    {
        LongIterator indexIds = indexingService.getIndexIds().longIterator();
        assertTrue( indexIds.hasNext() );
        return indexIds.next();
    }

    private CapableIndexDescriptor asCapableIndexDescriptor( IndexingService indexingService, long indexId ) throws IndexNotFoundKernelException
    {
        IndexProxy indexProxy = indexingService.getIndexProxy( indexId );
        return indexProxy.getDescriptor();
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

    private void createData( GraphDatabaseService database, int numberOfNodes )
    {
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = database.createNode( Label.label( FOOD_LABEL ), Label.label( CLOTHES_LABEL ),
                        Label.label( WEATHER_LABEL ) );
                node.setProperty( PROPERTY_NAME, "Node" + i );
                Relationship relationship = node.createRelationshipTo( node, RelationshipType.withName( FOOD_LABEL ) );
                relationship.setProperty( PROPERTY_NAME, "Relationship" + i );
                transaction.success();
            }
        }
    }

    private int getPropertyKeyId( String name )
    {
        try ( Transaction tx = database.beginTx() )
        {
            KernelTransaction transaction = ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency(
                    ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( true );
            return transaction.tokenRead().propertyKey( name );
        }
    }

    private int getLabelId( String name )
    {
        try ( Transaction tx = database.beginTx() )
        {
            KernelTransaction transaction = ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency(
                    ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( true );
            return transaction.tokenRead().nodeLabel( name );
        }
    }
}
