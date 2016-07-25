/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexProviderFactory;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;

public class IndexingServiceIntegrationTest
{
    private static final String FOOD_LABEL = "food";
    private static final String CLOTHES_LABEL = "clothes";
    private static final String WEATHER_LABEL = "weather";
    private static final String PROPERTY_NAME = "name";
    private static final SchemaIndexProvider.Descriptor indexDescriptor =
            LuceneSchemaIndexProviderFactory.PROVIDER_DESCRIPTOR;

    @Rule
    public EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    private GraphDatabaseService database;

    @Before
    public void setUp() {
        EphemeralFileSystemAbstraction fileSystem = fileSystemRule.get();
        database = new TestGraphDatabaseFactory()
                .setFileSystem( fileSystem )
                .addKernelExtension( new LuceneSchemaIndexProviderFactory() )
                .newImpermanentDatabase();
        createData( database, 100 );
    }

    @Test
    public void testManualIndexPopulation() throws IOException, IndexNotFoundKernelException, InterruptedException
    {
        IndexingService indexingService = getIndexingService( database );
        SchemaStore schemaStore = getSchemaStore( database );

        LabelTokenHolder labelTokenHolder = getLabelTokenHolder( database );
        PropertyKeyTokenHolder propertyKeyTokenHolder = getPropertyKeyTokenHolder( database );
        int foodId = labelTokenHolder.getIdByName( FOOD_LABEL );
        int propertyId = propertyKeyTokenHolder.getIdByName( PROPERTY_NAME );

        IndexRule rule = IndexRule.indexRule( schemaStore.nextId(), foodId, propertyId,
                indexDescriptor );
        indexingService.createIndexes( rule );
        IndexProxy indexProxy = indexingService.getIndexProxy( rule.getId() );

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

        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
        }

        IndexingService indexingService = getIndexingService( database );
        LabelTokenHolder labelTokenHolder = getLabelTokenHolder( database );
        PropertyKeyTokenHolder propertyKeyTokenHolder = getPropertyKeyTokenHolder( database );
        int clothedLabelId = labelTokenHolder.getIdByName( CLOTHES_LABEL );
        int weatherLabelId = labelTokenHolder.getIdByName( WEATHER_LABEL );
        int propertyId = propertyKeyTokenHolder.getIdByName( PROPERTY_NAME );

        IndexProxy clothesIndex = indexingService.getIndexProxy( new IndexDescriptor( clothedLabelId, propertyId) );
        IndexProxy weatherIndex = indexingService.getIndexProxy( new IndexDescriptor( weatherLabelId, propertyId) );
        assertEquals( InternalIndexState.ONLINE, clothesIndex.getState());
        assertEquals( InternalIndexState.ONLINE, weatherIndex.getState());
    }

    private PropertyKeyTokenHolder getPropertyKeyTokenHolder( GraphDatabaseService database )
    {
        return getDependencyResolver( database ).resolveDependency( PropertyKeyTokenHolder.class );
    }

    private void waitIndexOnline( IndexProxy indexProxy ) throws InterruptedException
    {
        while ( InternalIndexState.ONLINE != indexProxy.getState() )
        {
            Thread.sleep( 10 );
        }
    }

    private SchemaStore getSchemaStore( GraphDatabaseService database )
    {
        NeoStores neoStores = getDependencyResolver( database )
                .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        return neoStores.getSchemaStore();
    }

    private IndexingService getIndexingService( GraphDatabaseService database )
    {
        return getDependencyResolver(database).resolveDependency( IndexingService.class );
    }

    private LabelTokenHolder getLabelTokenHolder(GraphDatabaseService database)
    {
        return getDependencyResolver( database ).resolveDependency( LabelTokenHolder.class );
    }

    private DependencyResolver getDependencyResolver( GraphDatabaseService database )
    {
        return ((GraphDatabaseAPI)database).getDependencyResolver();
    }

    private void createData( GraphDatabaseService database, int numberOfNodes )
    {
        int index = 0;
        while (index < numberOfNodes)
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = database.createNode( Label.label( FOOD_LABEL ), Label.label( CLOTHES_LABEL ),
                        Label.label( WEATHER_LABEL ) );
                node.setProperty( PROPERTY_NAME, "Node" + index++ );
                transaction.success();
            }
        }
    }
}
