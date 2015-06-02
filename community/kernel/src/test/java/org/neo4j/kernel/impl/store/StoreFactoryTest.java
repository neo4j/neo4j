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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.PageCacheRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

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
        PageCache pageCache = pageCacheRule.getPageCache( fs );

        storeFactory = new StoreFactory( new File( "graph.db/neostore" ), new Config(), new DefaultIdGeneratorFactory(),
                pageCache, fs, NullLogProvider.getInstance(), new Monitors() );
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
        long[] lastCommittedTransaction = neostore.getLastCommittedTransaction();
        long[] txIdChecksum = new long[2];
        System.arraycopy( lastCommittedTransaction, 0, txIdChecksum, 0, 2 );
        assertArrayEquals( neostore.getUpgradeTransaction(), txIdChecksum );
    }
}
