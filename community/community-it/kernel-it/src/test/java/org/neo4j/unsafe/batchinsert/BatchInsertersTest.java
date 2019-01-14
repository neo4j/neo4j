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
package org.neo4j.unsafe.batchinsert;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProviderFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.unsafe.batchinsert.internal.FileSystemClosingBatchInserter;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.unsafe.batchinsert.BatchInserters.inserter;

public class BatchInsertersTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Test
    public void automaticallyCloseCreatedFileSystemOnShutdown() throws Exception
    {
        verifyInserterFileSystemClose( inserter( getStoreDir() ) );
        verifyInserterFileSystemClose( inserter( getStoreDir(), getConfig() ) );
        verifyInserterFileSystemClose( inserter( getStoreDir(), getConfig(), getKernelExtensions() ) );
    }

    @Test
    public void providedFileSystemNotClosedAfterShutdown() throws IOException
    {
        EphemeralFileSystemAbstraction fs = fileSystemRule.get();
        verifyProvidedFileSystemOpenAfterShutdown( inserter( getStoreDir(), fs ), fs );
        verifyProvidedFileSystemOpenAfterShutdown( inserter( getStoreDir(), fs, getConfig() ), fs );
        verifyProvidedFileSystemOpenAfterShutdown( inserter( getStoreDir(), fs, getConfig(), getKernelExtensions() ), fs );
    }

    private static Iterable<KernelExtensionFactory<?>> getKernelExtensions()
    {
        return Iterables.asIterable( new GenericNativeIndexProviderFactory() );
    }

    private static Map<String,String> getConfig()
    {
        return MapUtil.stringMap( default_schema_provider.name(), GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerName() );
    }

    private static void verifyProvidedFileSystemOpenAfterShutdown( BatchInserter inserter, EphemeralFileSystemAbstraction fileSystemAbstraction )
    {
        inserter.shutdown();
        assertFalse( fileSystemAbstraction.isClosed() );
    }

    private File getStoreDir()
    {
        return testDirectory.storeDir();
    }

    private static void verifyInserterFileSystemClose( BatchInserter inserter )
    {
        assertThat( "Expect specific implementation that will do required cleanups.", inserter,
                is( instanceOf( FileSystemClosingBatchInserter.class ) ) );
        inserter.shutdown();
    }
}
