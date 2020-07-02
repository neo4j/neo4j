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
package org.neo4j.kernel.api.impl.schema;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30.subProviderDirectoryStructure;
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

    private int numberValue = 1;
    private String stringValue = "string";
    private PointValue spatialValue = pointValue( CoordinateReferenceSystem.WGS84, 0.5, 0.5 );
    private DateValue temporalValue = DateValue.date( 2018, 3, 19 );

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.default_schema_provider, GraphDatabaseSettings.SchemaIndex.NATIVE30.providerName() );
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
                deleteIndexFilesFor( descriptor );
            }
            return builder;
        } );
    }

    private void verifyContent()
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1L, Iterators.stream( tx.schema().getIndexes( label ).iterator() ).count() );
            assertNotNull( tx.findNode( label, propKey, numberValue ) );
            assertNotNull( tx.findNode( label, propKey, stringValue ) );
            assertNotNull( tx.findNode( label, propKey, spatialValue ) );
            assertNotNull( tx.findNode( label, propKey, temporalValue ) );
            tx.commit();
        }
    }

    private void deleteIndexFilesFor( IndexProviderDescriptor descriptor )
    {
        Path databaseDirectory = db.databaseLayout().databaseDirectory();
        Path rootDirectory = subProviderDirectoryStructure( databaseDirectory ).forProvider( descriptor ).rootDirectory();
        File[] files = fs.listFiles( rootDirectory.toFile() );
        for ( File indexFile : files )
        {
            fs.deleteFile( indexFile );
        }
    }

    private void initializeIndexWithData()
    {
        createIndex();
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( label ).setProperty( propKey, numberValue );
            tx.createNode( label ).setProperty( propKey, stringValue );
            tx.createNode( label ).setProperty( propKey, spatialValue );
            tx.createNode( label ).setProperty( propKey, temporalValue );
            tx.commit();
        }
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( label ).on( propKey ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.commit();
        }
    }
}
