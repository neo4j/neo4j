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
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.EmbeddedDbmsRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.PointValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.kernel.api.impl.schema.NativeLuceneFusionIndexProviderFactory30.subProviderDirectoryStructure;
import static org.neo4j.values.storable.Values.pointValue;

public class FusionIndexIT
{
    @Rule
    public DbmsRule db = new EmbeddedDbmsRule()
            .withSetting( GraphDatabaseSettings.default_schema_provider, GraphDatabaseSettings.SchemaIndex.NATIVE30.providerName() );

    private DatabaseLayout databaseLayout;
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
        databaseLayout = db.databaseLayout();
        fs = db.getDependencyResolver().resolveDependency( FileSystemAbstraction.class );
    }

    @Test
    public void mustRebuildFusionIndexIfNativePartIsMissing()
    {
        // given
        initializeIndexWithDataAndShutdown();

        // when
        IndexProviderDescriptor descriptor = GenericNativeIndexProvider.DESCRIPTOR;
        deleteIndexFilesFor( descriptor );

        // then
        // ... should rebuild
        verifyContent();
    }

    @Test
    public void mustRebuildFusionIndexIfLucenePartIsMissing()
    {
        // given
        initializeIndexWithDataAndShutdown();

        // when
        IndexProviderDescriptor descriptor = LuceneIndexProvider.DESCRIPTOR;
        deleteIndexFilesFor( descriptor );

        // then
        // ... should rebuild
        verifyContent();
    }

    @Test
    public void mustRebuildFusionIndexIfCompletelyMissing()
    {
        // given
        initializeIndexWithDataAndShutdown();

        // when
        IndexProviderDescriptor luceneDescriptor = LuceneIndexProvider.DESCRIPTOR;
        IndexProviderDescriptor nativeDescriptor = GenericNativeIndexProvider.DESCRIPTOR;
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
            tx.commit();
        }
    }

    private void deleteIndexFilesFor( IndexProviderDescriptor descriptor )
    {
        File databaseDirectory = this.databaseLayout.databaseDirectory();
        File rootDirectory = subProviderDirectoryStructure( databaseDirectory ).forProvider( descriptor ).rootDirectory();
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
            tx.commit();
        }
        db.shutdown();
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( propKey ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.commit();
        }
    }
}
