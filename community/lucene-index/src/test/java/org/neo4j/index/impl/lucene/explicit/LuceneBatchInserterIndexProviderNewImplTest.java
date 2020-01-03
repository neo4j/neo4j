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
package org.neo4j.index.impl.lucene.explicit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.impl.schema.LuceneIndexProviderFactory;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class LuceneBatchInserterIndexProviderNewImplTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Test
    void createBatchIndexFromAnyIndexStoreProvider() throws Exception
    {
        createEndCloseIndexProvider( BatchInserters.inserter( getStoreDir() ) );
        createEndCloseIndexProvider( BatchInserters.inserter( getStoreDir(), fileSystem ) );
        createEndCloseIndexProvider( BatchInserters.inserter( getStoreDir(), getConfig() ) );
        createEndCloseIndexProvider( BatchInserters.inserter( getStoreDir(), getConfigWithProvider(), getExtensions() ) );
        createEndCloseIndexProvider( BatchInserters.inserter( getStoreDir(), fileSystem, getConfig() ) );
        createEndCloseIndexProvider( BatchInserters.inserter( getStoreDir(), fileSystem, getConfigWithProvider(), getExtensions() ) );
    }

    private static void createEndCloseIndexProvider( BatchInserter inserter )
    {
        LuceneBatchInserterIndexProviderNewImpl provider = new LuceneBatchInserterIndexProviderNewImpl( inserter );
        provider.shutdown();
        inserter.shutdown();
    }

    private static Iterable<KernelExtensionFactory<?>> getExtensions()
    {
        return Iterables.asIterable( new LuceneIndexProviderFactory() );
    }

    private static Map<String,String> getConfigWithProvider()
    {
        return getConfig( GraphDatabaseSettings.default_schema_provider.name(), LuceneIndexProviderFactory.PROVIDER_DESCRIPTOR.name() );
    }

    private static Map<String,String> getConfig( String... entries )
    {
        return MapUtil.stringMap( entries );
    }

    private File getStoreDir()
    {
        return testDirectory.databaseDir();
    }
}
