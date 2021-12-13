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
package org.neo4j.kernel.internal;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.CommonDatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.diagnostics.providers.StoreFilesDiagnostics;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.ByteUnit.bytesToString;

@Neo4jLayoutExtension
class KernelDiagnosticsIT
{
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private Neo4jLayout neo4jLayout;

    @Test
    void shouldIncludeNativeIndexFilesInTotalMappedSize()
    {
        for ( GraphDatabaseSettings.SchemaIndex schemaIndex : GraphDatabaseSettings.SchemaIndex.values() )
        {
            // given
            Neo4jLayout layout = neo4jLayout;
            createIndexInIsolatedDbInstance( layout.homeDirectory(), schemaIndex );

            // when
            DatabaseLayout databaseLayout = layout.databaseLayout( DEFAULT_DATABASE_NAME );
            StorageEngineFactory storageEngineFactory = StorageEngineFactory.defaultStorageEngine();
            StoreFilesDiagnostics files = new StoreFilesDiagnostics( storageEngineFactory, fs, databaseLayout );
            SizeCapture capture = new SizeCapture();
            files.dump( capture::log );
            assertNotNull( capture.size );

            // then
            long expected = manuallyCountTotalMappedFileSize( databaseLayout.databaseDirectory() );
            assertEquals( bytesToString( expected ), capture.size );
        }
    }

    private static void createIndexInIsolatedDbInstance( Path homeDir, GraphDatabaseSettings.SchemaIndex index )
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( homeDir )
                        .setConfig( GraphDatabaseSettings.default_schema_provider, index.providerName() )
                        .build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            Label label = Label.label( "Label-" + index.providerName() );
            String key = "key";
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < 100; i++ )
                {
                    tx.createNode( label ).setProperty( key, i );
                }
                tx.commit();
            }
            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().indexFor( label ).on( key ).create();
                tx.commit();
            }
            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().awaitIndexesOnline( 2, MINUTES );
                tx.commit();
            }
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private static long manuallyCountTotalMappedFileSize( Path dbDir )
    {
        MutableLong result = new MutableLong();
        NativeIndexFileFilter nativeIndexFilter = new NativeIndexFileFilter( dbDir );
        manuallyCountTotalMappedFileSize( dbDir, result, nativeIndexFilter );
        return result.getValue();
    }

    private static void manuallyCountTotalMappedFileSize( Path dir, MutableLong result, NativeIndexFileFilter nativeIndexFilter )
    {
        Set<String> storeFiles = Stream.of( StoreType.values() ).map( type -> type.getDatabaseFile().getName() ).collect( Collectors.toSet() );
        try ( DirectoryStream<Path> paths = Files.newDirectoryStream( dir ) )
        {
            for ( Path path : paths )
            {
                if ( Files.isDirectory( path ) )
                {
                    manuallyCountTotalMappedFileSize( path, result, nativeIndexFilter );
                }
                else if ( storeFiles.contains( path.getFileName().toString() ) ||
                        path.getFileName().toString().equals( CommonDatabaseFile.LABEL_SCAN_STORE.getName() ) ||
                        path.getFileName().toString().equals( CommonDatabaseFile.RELATIONSHIP_TYPE_SCAN_STORE.getName() ) ||
                        nativeIndexFilter.test( path ) )
                {
                    try
                    {
                        result.add( Files.size( path ) );
                    }
                    catch ( IOException ignored )
                    {
                        // Preserve behaviour of File.length()
                    }
                }
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static class SizeCapture
    {
        private String size;

        public void log( String message )
        {
            if ( message.contains( "Total size of mapped files" ) )
            {
                int beginPos = message.lastIndexOf( ": " );
                Assertions.assertTrue( beginPos != -1 );
                size = message.substring( beginPos + 2 );
            }
        }
    }
}
