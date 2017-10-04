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
package org.neo4j.index.impl.lucene.explicit;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class LuceneBatchInserterIndexProviderNewImplTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Test
    public void createBatchIndexFromAnyIndexStoreProvider() throws Exception
    {
        createEndCloseIndexProvider( BatchInserters.inserter( getStoreDir() ) );
        createEndCloseIndexProvider( BatchInserters.inserter( getStoreDir(), fileSystemRule.get() ) );
        createEndCloseIndexProvider( BatchInserters.inserter( getStoreDir(), getConfig() ) );
        createEndCloseIndexProvider( BatchInserters.inserter( getStoreDir(), getConfig(), getExtensions() ) );
        createEndCloseIndexProvider( BatchInserters.inserter( getStoreDir(), fileSystemRule.get(), getConfig() ) );
        createEndCloseIndexProvider( BatchInserters.inserter( getStoreDir(), fileSystemRule.get(), getConfig(),
                getExtensions() ) );
    }

    private void createEndCloseIndexProvider( BatchInserter inserter )
    {
        LuceneBatchInserterIndexProviderNewImpl provider = new LuceneBatchInserterIndexProviderNewImpl( inserter );
        provider.shutdown();
        inserter.shutdown();
    }

    private Iterable<KernelExtensionFactory<?>> getExtensions()
    {
        return Iterables.asIterable( new InMemoryIndexProviderFactory() );
    }

    private Map<String,String> getConfig()
    {
        return MapUtil.stringMap();
    }

    private File getStoreDir()
    {
        return testDirectory.graphDbDir();
    }
}
