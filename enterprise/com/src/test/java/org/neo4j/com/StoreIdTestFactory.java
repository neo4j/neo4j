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
