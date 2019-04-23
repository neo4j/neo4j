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
package org.neo4j.kernel.internal;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.diagnostics.providers.StoreFilesDiagnostics;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.logging.Logger;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.ByteUnit.bytesToString;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class KernelDiagnosticsIT
{
    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fs;

    @Test
    void shouldIncludeNativeIndexFilesInTotalMappedSize()
    {
        int i = 0;
        for ( GraphDatabaseSettings.SchemaIndex schemaIndex : GraphDatabaseSettings.SchemaIndex.values() )
        {
            // given
            File storeDir = directory.storeDir( String.valueOf( i++ ) );
            createIndexInIsolatedDbInstance( storeDir, schemaIndex );

            // when
            DatabaseLayout databaseLayout = DatabaseLayout.of( storeDir, DEFAULT_DATABASE_NAME );
            StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine();
            StoreFilesDiagnostics files = new StoreFilesDiagnostics( storageEngineFactory, fs, databaseLayout );
            SizeCapture capture = new SizeCapture();
            files.dump( capture );
            assertNotNull( capture.size );

            // then
            long expected = manuallyCountTotalMappedFileSize( databaseLayout.databaseDirectory() );
            assertEquals( bytesToString( expected ), capture.size );
        }
    }

    private static void createIndexInIsolatedDbInstance( File storeDir, GraphDatabaseSettings.SchemaIndex index )
    {
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( storeDir )
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
                    db.createNode( label ).setProperty( key, i );
                }
                tx.success();
            }
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().indexFor( label ).on( key ).create();
                tx.success();
            }
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 1, MINUTES );
                tx.success();
            }
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private static long manuallyCountTotalMappedFileSize( File dbDir )
    {
        MutableLong result = new MutableLong();
        NativeIndexFileFilter nativeIndexFilter = new NativeIndexFileFilter( dbDir );
        manuallyCountTotalMappedFileSize( dbDir, result, nativeIndexFilter );
        return result.getValue();
    }

    private static void manuallyCountTotalMappedFileSize( File dir, MutableLong result, NativeIndexFileFilter nativeIndexFilter )
    {
        Set<String> storeFiles = Stream.of( StoreType.values() ).map( type -> type.getDatabaseFile().getName() ).collect( Collectors.toSet() );
        for ( File file : dir.listFiles() )
        {
            if ( file.isDirectory() )
            {
                manuallyCountTotalMappedFileSize( file, result, nativeIndexFilter );
            }
            else if ( storeFiles.contains( file.getName() ) || file.getName().equals( DatabaseFile.LABEL_SCAN_STORE.getName() ) ||
                    nativeIndexFilter.accept( file ) )
            {
                result.add( file.length() );
            }
        }
    }

    private class SizeCapture implements Logger
    {
        private String size;

        @Override
        public void log( @Nonnull String message )
        {
            if ( message.contains( "Total size of mapped files" ) )
            {
                int beginPos = message.lastIndexOf( ": " );
                Assertions.assertTrue( beginPos != -1 );
                size = message.substring( beginPos + 2 );
            }
        }

        @Override
        public void log( @Nonnull String message, @Nonnull Throwable throwable )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void log( @Nonnull String format, @Nullable Object... arguments )
        {
            log( format( format, arguments ) );
        }

        @Override
        public void bulk( @Nonnull Consumer<Logger> consumer )
        {
            throw new UnsupportedOperationException();
        }
    }
}
