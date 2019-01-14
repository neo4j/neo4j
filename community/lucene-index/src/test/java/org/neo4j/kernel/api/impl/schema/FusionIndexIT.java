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
package org.neo4j.kernel.api.impl.schema;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.index.schema.NumberIndexProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.PointValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.kernel.api.impl.schema.NativeLuceneFusionIndexProviderFactory20.subProviderDirectoryStructure;
import static org.neo4j.values.storable.Values.pointValue;

public class FusionIndexIT
{
    @Rule
    public DatabaseRule db = new EmbeddedDatabaseRule()
            .withSetting( GraphDatabaseSettings.default_schema_provider, GraphDatabaseSettings.SchemaIndex.NATIVE20.providerName() );

    private File storeDir;
    private final Label label = Label.label( "label" );
    private final String propKey = "propKey";
    private FileSystemAbstraction fs;
    private int numberValue = 1;
    private String stringValue = "string";
    private PointValue spatialValue = pointValue( CoordinateReferenceSystem.WGS84, 0.5, 0.5 );
    private DateValue temporalValue = DateValue.date( 2018, 3, 19 );

    @Before
    public void setup()
    {
        storeDir = db.getStoreDir();
        fs = db.getDependencyResolver().resolveDependency( FileSystemAbstraction.class );
    }

    @Test
    public void mustRebuildFusionIndexIfNativePartIsMissing() throws IOException
    {
        // given
        initializeIndexWithDataAndShutdown();

        // when
        IndexProvider.Descriptor descriptor = NumberIndexProvider.NATIVE_PROVIDER_DESCRIPTOR;
        deleteIndexFilesFor( descriptor );

        // then
        // ... should rebuild
        verifyContent();
    }

    @Test
    public void mustRebuildFusionIndexIfLucenePartIsMissing() throws IOException
    {
        // given
        initializeIndexWithDataAndShutdown();

        // when
        IndexProvider.Descriptor descriptor = LuceneIndexProviderFactory.PROVIDER_DESCRIPTOR;
        deleteIndexFilesFor( descriptor );

        // then
        // ... should rebuild
        verifyContent();
    }

    @Test
    public void mustRebuildFusionIndexIfCompletelyMissing() throws IOException
    {
        // given
        initializeIndexWithDataAndShutdown();

        // when
        IndexProvider.Descriptor luceneDescriptor = LuceneIndexProviderFactory.PROVIDER_DESCRIPTOR;
        IndexProvider.Descriptor nativeDescriptor = NumberIndexProvider.NATIVE_PROVIDER_DESCRIPTOR;
        deleteIndexFilesFor( luceneDescriptor );
        deleteIndexFilesFor( nativeDescriptor );

        // then
        // ... should rebuild
        verifyContent();
    }

    private void verifyContent()
    {
        GraphDatabaseAPI newDb = db.getGraphDatabaseAPI();
        try ( Transaction tx = newDb.beginTx() )
        {
            assertEquals( 1L, Iterators.stream( newDb.schema().getIndexes( label ).iterator() ).count() );
            assertNotNull( newDb.findNode( label, propKey, numberValue ) );
            assertNotNull( newDb.findNode( label, propKey, stringValue ) );
            assertNotNull( newDb.findNode( label, propKey, spatialValue ) );
            assertNotNull( newDb.findNode( label, propKey, temporalValue ) );
            tx.success();
        }
    }

    private void deleteIndexFilesFor( IndexProvider.Descriptor descriptor )
    {
        File rootDirectory = subProviderDirectoryStructure( storeDir ).forProvider( descriptor ).rootDirectory();
        File[] files = fs.listFiles( rootDirectory );
        for ( File indexFile : files )
        {
            fs.deleteFile( indexFile );
        }
    }

    private void initializeIndexWithDataAndShutdown()
    {
        createIndex();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label ).setProperty( propKey, numberValue );
            db.createNode( label ).setProperty( propKey, stringValue );
            db.createNode( label ).setProperty( propKey, spatialValue );
            db.createNode( label ).setProperty( propKey, temporalValue );
            tx.success();
        }
        db.shutdown();
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( propKey ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }
    }
}
