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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.token.TokenHolders;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.index_background_sampling_enabled;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.ArrayUtil.single;
import static org.neo4j.logging.AssertableLogProvider.inLog;

@ExtendWith( EphemeralFileSystemExtension.class )
class IndexStatisticsIT
{
    private static final Label ALIEN = label( "Alien" );
    private static final String SPECIMEN = "specimen";

    @Inject
    private EphemeralFileSystemAbstraction fs;
    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private GraphDatabaseService db;
    private DatabaseManagementService managementService;

    @BeforeEach
    void before()
    {
        startDb();
    }

    @AfterEach
    void after()
    {
        managementService.shutdown();
    }

    @Test
    void shouldRecoverIndexCountsBySamplingThemOnStartup()
    {
        // given some aliens in a database
        createAliens();

        // that have been indexed
        awaitIndexOnline( indexAliensBySpecimen() );

        // where ALIEN and SPECIMEN are both the first ids of their kind
        IndexDescriptor index = TestIndexDescriptorFactory.forLabel( labelId( ALIEN ), pkId( SPECIMEN ) );
        SchemaRuleAccess schemaRuleAccess =
                SchemaRuleAccess.getSchemaRuleAccess( neoStores().getSchemaStore(), resolveDependency( TokenHolders.class ) );
        long indexId = single( schemaRuleAccess.indexGetForSchema( index ) ).getId();

        // for which we don't have index counts
        resetIndexCounts( indexId );

        // when we shutdown the database and restart it
        restart();

        // then we should have re-sampled the index
        IndexStatisticsStore indexStatisticsStore = indexStatistics();
        var indexSample = indexStatisticsStore.indexSample( indexId );
        assertEquals( 0, indexSample.updates() );
        assertEquals( 32, indexSample.indexSize() );
        assertEquals( 16, indexSample.uniqueValues() );
        assertEquals( 32, indexSample.sampleSize() );
        // and also
        assertLogExistsForRecoveryOn( ":Alien(specimen)" );
    }

    private void assertLogExistsForRecoveryOn( String labelAndProperty )
    {
        logProvider.assertAtLeastOnce(
                inLog( IndexSamplingController.class ).debug( containsString( "Recovering index sampling for index %s" ), labelAndProperty )
        );
    }

    private int labelId( Label alien )
    {
        try ( Transaction tx = db.beginTx() )
        {
            return ((InternalTransaction) tx).kernelTransaction().tokenRead().nodeLabel( alien.name() );
        }
    }

    private int pkId( String propertyName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            return ((InternalTransaction) tx).kernelTransaction().tokenRead().propertyKey( propertyName );
        }
    }

    private void createAliens()
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 32; i++ )
            {
                Node alien = tx.createNode( ALIEN );
                alien.setProperty( SPECIMEN, i / 2 );
            }
            tx.commit();
        }
    }

    private void awaitIndexOnline( IndexDefinition definition )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( definition, 10, TimeUnit.SECONDS );
            tx.commit();
        }
    }

    private IndexDefinition indexAliensBySpecimen()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition definition = tx.schema().indexFor( ALIEN ).on( SPECIMEN ).create();
            tx.commit();
            return definition;
        }
    }

    private void resetIndexCounts( long indexId )
    {
        indexStatistics().replaceStats( indexId, new IndexSample( 0, 0, 0 ) );
    }

    private <T> T resolveDependency( Class<T> clazz )
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( clazz );
    }

    private NeoStores neoStores()
    {
        return resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
    }

    private IndexStatisticsStore indexStatistics()
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( IndexStatisticsStore.class );
    }

    private void startDb()
    {
        managementService = new TestDatabaseManagementServiceBuilder()
                .setInternalLogProvider( logProvider )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs ) )
                .impermanent()
                .setConfig( index_background_sampling_enabled, false )
                .build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void restart()
    {
        managementService.shutdown();
        startDb();
    }
}
