/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.convert;

import java.io.File;
import java.io.IOException;
import java.time.Clock;

import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.time.Clocks;

import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.RANDOM_NUMBER;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.MetaDataStore.getRecord;

public class GenerateClusterSeedCommand
{
    private final Clock clock;

    public GenerateClusterSeedCommand()
    {
        this( Clocks.systemClock() );
    }

    GenerateClusterSeedCommand( Clock clock )
    {
        this.clock = clock;
    }

    public ClusterSeed generate( File databaseDir ) throws IOException
    {
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File metadataStore = new File( databaseDir, MetaDataStore.DEFAULT_NAME );
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs ) )
        {
            long lastTxId = getRecord( pageCache, metadataStore, LAST_TRANSACTION_ID );
            StoreId before = storeId( metadataStore, pageCache, getRecord( pageCache, metadataStore, UPGRADE_TIME ),
                    getRecord( pageCache, metadataStore, UPGRADE_TRANSACTION_ID ) );
            StoreId after = storeId( metadataStore, pageCache, clock.millis(), lastTxId );

            return new ClusterSeed( before, after, lastTxId );
        }
    }

    public static StoreId storeId( File metadataStore, PageCache pageCache, long upgradeTime, long upgradeId ) throws IOException
    {
        long creationTime = getRecord( pageCache, metadataStore, TIME );
        long randomNumber = getRecord( pageCache, metadataStore, RANDOM_NUMBER );
        return new StoreId( creationTime, randomNumber, upgradeTime, upgradeId );
    }
}
