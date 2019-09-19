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
package org.neo4j.kernel.recovery;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogHeaderVisitor;
import org.neo4j.storageengine.api.StoreId;

import static org.neo4j.util.FeatureToggles.flag;

/**
 * Helper to validate that the store id of the transaction logs match the current store we are doing recovery on
 */
class StoreIdValidator implements LogHeaderVisitor
{
    private static final boolean ignoreStoreId = flag( StoreIdValidator.class, "ignoreStoreId", false );

    private final StoreId storeStoreId;

    StoreIdValidator( StoreId storeStoreId )
    {
        this.storeStoreId = storeStoreId;
    }

    @Override
    public boolean visit( LogHeader logHeader, LogPosition position, long firstTransactionIdInLog, long lastTransactionIdInLog )
    {
        if ( ignoreStoreId )
        {
            return false; // false will stop the visitor
        }
//        if ( StoreId.DEFAULT.equals( logHeader.storeId ) )
//        {
//            return false; // Old log format do not contain store id so we can't check consistency
//        }
        if ( !storeStoreId.equals( logHeader.storeId ) )
        {
            throw new RuntimeException( "Mismatching store id. Store StoreId: " + storeStoreId +
                    ". Transaction log StoreId: " + logHeader.storeId );
        }
        return true;
    }
}
