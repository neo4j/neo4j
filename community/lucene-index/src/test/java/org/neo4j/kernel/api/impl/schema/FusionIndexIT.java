/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.index.schema.NativeSchemaNumberIndexProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FusionIndexIT
{
    @Rule
    public DatabaseRule db = new EmbeddedDatabaseRule();

    private File storeDir;
    private Label label = Label.label( "label" );
    private String propKey = "propKey";
    private FileSystemAbstraction fs;

    @Before
    public void setup()
    {
        storeDir = db.getStoreDir();
        fs = db.getDependencyResolver().resolveDependency( FileSystemAbstraction.class );
    }

    @Test
    public void mustRebuildFusionIndexIfNativePartIsMissing() throws Exception
    {
        // given
        initializeIndexWithDataAndShutdown();

        // when
        SchemaIndexProvider.Descriptor descriptor = NativeSchemaNumberIndexProvider.NATIVE_PROVIDER_DESCRIPTOR;
        deleteIndexFilesFor( descriptor );

        // then
        // ... should rebuild
        verifyContent();
    }

    @Test
    public void mustRebuildFusionIndexIfLucenePartIsMissing() throws Exception
    {
        // given
        initializeIndexWithDataAndShutdown();

        // when
        SchemaIndexProvider.Descriptor descriptor = LuceneSchemaIndexProviderFactory.PROVIDER_DESCRIPTOR;
        deleteIndexFilesFor( descriptor );

        // then
        // ... should rebuild
        verifyContent();
    }

    @Test
    public void mustRebuildFusionIndexIfCompletelyMissing() throws Exception
    {
        // given
        initializeIndexWithDataAndShutdown();

        // when
        SchemaIndexProvider.Descriptor luceneDescriptor = LuceneSchemaIndexProviderFactory.PROVIDER_DESCRIPTOR;
        SchemaIndexProvider.Descriptor nativeDescriptor = NativeSchemaNumberIndexProvider.NATIVE_PROVIDER_DESCRIPTOR;
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
            assertNotNull( newDb.findNode( label, propKey, 1 ) );
            assertNotNull( newDb.findNode( label, propKey, "string" ) );
            tx.success();
        }
    }

    private void deleteIndexFilesFor( SchemaIndexProvider.Descriptor descriptor )
    {
        File nativeIndexDirectory = SchemaIndexProvider.getSchemaIndexStoreDirectory( storeDir, descriptor );
        File[] files = fs.listFiles( nativeIndexDirectory );
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
            db.createNode( label ).setProperty( propKey, 1 );
            db.createNode( label ).setProperty( propKey, "string" );
            tx.success();
        }
        db.shutdown();
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition indexDefinition = db.schema().indexFor( label ).on( propKey ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }
    }
}
