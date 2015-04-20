/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.PageCacheRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class StoreFactoryTest
{
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private StoreFactory storeFactory;
    private NeoStore neostore;

    @Before
    public void setup()
    {
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        Map<String, String> configParams = stringMap(
                GraphDatabaseSettings.neo_store.name(), "graph.db/neostore" );
        PageCache pageCache = pageCacheRule.getPageCache( fs );

        storeFactory = new StoreFactory( new Config( configParams ), new DefaultIdGeneratorFactory(),
                pageCache, fs, StringLogger.DEV_NULL, new Monitors() );
    }

    @After
    public void teardown()
    {
        neostore.close();
    }

    @Test
    public void shouldHaveSameCreationTimeAndUpgradeTimeOnStartup() throws Exception
    {
        // When
        neostore = storeFactory.createNeoStore();

        // Then
        assertThat( neostore.getUpgradeTime(), equalTo( neostore.getCreationTime() ) );
    }

    @Test
    public void shouldHaveSameCommittedTransactionAndUpgradeTransactionOnStartup() throws Exception
    {
        // When
        neostore = storeFactory.createNeoStore();

        // Then
        assertArrayEquals( neostore.getUpgradeTransaction(), neostore.getLastCommittedTransaction() );
    }
}
