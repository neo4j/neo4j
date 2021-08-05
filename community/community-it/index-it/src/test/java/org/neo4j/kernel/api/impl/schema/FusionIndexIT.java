/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api.impl.schema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.PointValue;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30.subProviderDirectoryStructure;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;
import static org.neo4j.values.storable.Values.pointValue;

@DbmsExtension( configurationCallback = "configure" )
public class FusionIndexIT
{
    private final Label label = Label.label( "label" );
    private final String propKey = "propKey";

    @Inject
    private DbmsController controller;
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private FileSystemAbstraction fs;

    private final int numberValue = 1;
    private final String stringValue = "string";
    private final PointValue spatialValue = pointValue( CoordinateReferenceSystem.WGS84, 0.5, 0.5 );
    private final DateValue temporalValue = DateValue.date( 2018, 3, 19 );
    private final String indexName = "some_index_name";

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.default_schema_provider, GraphDatabaseSettings.SchemaIndex.NATIVE30.providerName() );
    }

    @Test
    void shouldKeepIndexSamplesUpdated()
    {
        // Given
        createIndex();
        assertThat( indexSample() ).isEqualTo( new IndexSample( 0, 0, 0, 0 ) );

        // When
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( label ).setProperty( propKey, numberValue );
            tx.createNode( label ).setProperty( propKey, stringValue );
            tx.createNode( label ).setProperty( propKey, spatialValue );
            tx.createNode( label ).setProperty( propKey, temporalValue );
            tx.commit();
        }

        // Then
        assertEventually( this::indexSample, equalityCondition( new IndexSample( 4, 4, 4 ) ), 1, MINUTES );
    }

    @Test
    void mustRebuildFusionIndexIfNativePartIsMissing()
    {
        // given
        initializeIndexWithData();

        // when
        deleteIndexFilesForProviderAndRestartDbms( GenericNativeIndexProvider.DESCRIPTOR );

        // then
        // ... should rebuild
        verifyContent();
    }

    @Test
    void mustRebuildFusionIndexIfLucenePartIsMissing()
    {
        // given
        initializeIndexWithData();

        // when
        deleteIndexFilesForProviderAndRestartDbms( LuceneIndexProvider.DESCRIPTOR );

        // then
        // ... should rebuild
        verifyContent();
    }

    @Test
    void mustRebuildFusionIndexIfCompletelyMissing()
    {
        // given
        initializeIndexWithData();

        // when
        deleteIndexFilesForProviderAndRestartDbms( LuceneIndexProvider.DESCRIPTOR, GenericNativeIndexProvider.DESCRIPTOR );

        // then
        // ... should rebuild
        verifyContent();
    }

    private void deleteIndexFilesForProviderAndRestartDbms( IndexProviderDescriptor ... descriptors )
    {
        controller.restartDbms( db.databaseName() , builder ->
        {
            for ( IndexProviderDescriptor descriptor : descriptors )
            {
                try
                {
                    deleteIndexFilesFor( descriptor );
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( e );
                }
            }
            return builder;
        } );
    }

    private void verifyContent()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 30, TimeUnit.SECONDS );
            assertEquals( 1L, Iterators.stream( tx.schema().getIndexes( label ).iterator() ).count() );
            assertNotNull( tx.findNode( label, propKey, numberValue ) );
            assertNotNull( tx.findNode( label, propKey, stringValue ) );
            assertNotNull( tx.findNode( label, propKey, spatialValue ) );
            assertNotNull( tx.findNode( label, propKey, temporalValue ) );
            assertThat( indexSample() ).isEqualTo( new IndexSample( 4, 4, 4, 0 ) );
            tx.commit();
        }
    }

    private void deleteIndexFilesFor( IndexProviderDescriptor descriptor ) throws IOException
    {
        Path databaseDirectory = db.databaseLayout().databaseDirectory();
        Path rootDirectory = subProviderDirectoryStructure( databaseDirectory ).forProvider( descriptor ).rootDirectory();
        Path[] files = fs.listFiles( rootDirectory );
        for ( Path indexFile : files )
        {
            fs.deleteRecursively( indexFile );
        }
    }

    private void initializeIndexWithData()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( label ).setProperty( propKey, numberValue );
            tx.createNode( label ).setProperty( propKey, stringValue );
            tx.createNode( label ).setProperty( propKey, spatialValue );
            tx.createNode( label ).setProperty( propKey, temporalValue );
            tx.commit();
        }
        createIndex();
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( label ).on( propKey ).withName( indexName ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 30, TimeUnit.SECONDS );
            tx.commit();
        }
    }

    private IndexSample indexSample()
    {
        try ( var tx = db.beginTx() )
        {
            var index = (IndexDefinitionImpl) tx.schema().getIndexByName( indexName );
            return db.getDependencyResolver().resolveDependency( IndexStatisticsStore.class ).indexSample( index.getIndexReference().getId() );
        }
    }
}
