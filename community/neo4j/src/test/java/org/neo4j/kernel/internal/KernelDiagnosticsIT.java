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
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Format;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.logging.Logger;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class KernelDiagnosticsIT
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

    @Test
    public void shouldIncludeNativeIndexFilesInTotalMappedSize()
    {
        File storeDir = directory.directory();
        int i = 0;
        for ( GraphDatabaseSettings.SchemaIndex schemaIndex : GraphDatabaseSettings.SchemaIndex.values() )
        {
            // given
            File dbDir = new File( storeDir, String.valueOf( i++ ) );
            createIndexInIsolatedDbInstance( dbDir, schemaIndex );

            // when
            KernelDiagnostics.StoreFiles files = new KernelDiagnostics.StoreFiles( dbDir );
            SizeCapture capture = new SizeCapture();
            files.dump( capture );
            assertNotNull( capture.size );

            // then
            long expected = manuallyCountTotalMappedFileSize( dbDir );
            assertEquals( Format.bytes( expected ), capture.size );
        }
    }

    private void createIndexInIsolatedDbInstance( File storeDir, GraphDatabaseSettings.SchemaIndex index )
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.default_schema_provider, index.providerName() )
                .newGraphDatabase();
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
            db.shutdown();
        }
    }

    private long manuallyCountTotalMappedFileSize( File dbDir )
    {
        MutableLong result = new MutableLong();
        NativeIndexFileFilter nativeIndexFilter = new NativeIndexFileFilter( dbDir );
        manuallyCountTotalMappedFileSize( dbDir, result, nativeIndexFilter );
        return result.getValue();
    }

    private void manuallyCountTotalMappedFileSize( File dir, MutableLong result, NativeIndexFileFilter nativeIndexFilter )
    {
        for ( File file : dir.listFiles() )
        {
            if ( file.isDirectory() )
            {
                manuallyCountTotalMappedFileSize( file, result, nativeIndexFilter );
            }
            else if ( StoreType.canBeManagedByPageCache( file.getName() ) || nativeIndexFilter.accept( file ) )
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
                assertTrue( beginPos != -1 );
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
