/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.com;

import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;

public class StoreIdTestFactory
{
    private static final RecordFormats format = RecordFormatSelector.defaultFormat();

    private StoreIdTestFactory()
    {
    }

    private static long currentStoreVersionAsLong()
    {
        return MetaDataStore.versionStringToLong( format.storeVersion() );
    }

    public static StoreId newStoreIdForCurrentVersion()
    {
        return new StoreId( currentStoreVersionAsLong() );
    }

    public static StoreId newStoreIdForCurrentVersion( long creationTime, long randomId, long upgradeTime, long
            upgradeId )
    {
        return new StoreId( creationTime, randomId, MetaDataStore.versionStringToLong( format.storeVersion() ),
                upgradeTime, upgradeId );
    }
}
