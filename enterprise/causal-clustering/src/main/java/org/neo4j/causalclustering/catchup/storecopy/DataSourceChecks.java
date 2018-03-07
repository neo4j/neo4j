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
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;

class DataSourceChecks
{
    private DataSourceChecks()
    {
    }

    static boolean isTransactionWithinReach( long requiredTxId, CheckPointer checkpointer )
    {
        if ( isWithinLastCheckPoint( requiredTxId, checkpointer ) )
        {
            return true;
        }
        else
        {
            try
            {
                checkpointer.tryCheckPoint( new SimpleTriggerInfo( "Store file copy" ) );
                return isWithinLastCheckPoint( requiredTxId, checkpointer );
            }
            catch ( IOException e )
            {
                return false;
            }
        }
    }

    private static boolean isWithinLastCheckPoint( long atLeast, CheckPointer checkPointer )
    {
        return checkPointer.lastCheckPointedTransactionId() >= atLeast;
    }

    static boolean hasSameStoreId( StoreId storeId, NeoStoreDataSource dataSource )
    {
        return storeId.equalToKernelStoreId( dataSource.getStoreId() );
    }
}
