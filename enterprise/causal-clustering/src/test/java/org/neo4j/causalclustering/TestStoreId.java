/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.RANDOM_NUMBER;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TRANSACTION_ID;

public class TestStoreId
{
    private TestStoreId()
    {
    }

    public static void assertAllStoresHaveTheSameStoreId( List<File> coreStoreDirs, FileSystemAbstraction fs )
            throws IOException
    {
        Set<StoreId> storeIds = getStoreIds( coreStoreDirs, fs );
        assertEquals( "Store Ids " + storeIds, 1, storeIds.size() );
    }

    public static Set<StoreId> getStoreIds( List<File> coreStoreDirs, FileSystemAbstraction fs ) throws IOException
    {
        Set<StoreId> storeIds = new HashSet<>();
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs ) )
        {
            for ( File coreStoreDir : coreStoreDirs )
            {
                storeIds.add( doReadStoreId( coreStoreDir, pageCache ) );
            }
        }

        return storeIds;
    }

    private static StoreId doReadStoreId( File coreStoreDir, PageCache pageCache ) throws IOException
    {
        File metadataStore = new File( coreStoreDir, MetaDataStore.DEFAULT_NAME );

        long creationTime = MetaDataStore.getRecord( pageCache, metadataStore, TIME );
        long randomNumber = MetaDataStore.getRecord( pageCache, metadataStore, RANDOM_NUMBER );
        long upgradeTime = MetaDataStore.getRecord( pageCache, metadataStore, UPGRADE_TIME );
        long upgradeId = MetaDataStore.getRecord( pageCache, metadataStore, UPGRADE_TRANSACTION_ID );

        return new StoreId( creationTime, randomNumber, upgradeTime, upgradeId );
    }
}
