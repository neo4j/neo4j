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
package org.neo4j.bolt.v3.runtime;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltQueryExecutor;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltResultHandle;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.runtime.statemachine.impl.AbstractTransactionStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.impl.BoltAdapterSubscriber;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

public class TransactionStateMachineV3SPI extends AbstractTransactionStateMachineSPI
{
    public TransactionStateMachineV3SPI( BoltGraphDatabaseServiceSPI boltGraphDatabaseServiceSPI, BoltChannel boltChannel, SystemNanoClock clock,
            StatementProcessorReleaseManager resourceReleaseManger )
    {
        super( boltGraphDatabaseServiceSPI, boltChannel, clock, resourceReleaseManger );
    }

    @Override
    public BoltTransaction beginTransaction( LoginContext loginContext, List<Bookmark> bookmarks, Duration txTimeout, AccessMode accessMode,
            Map<String,Object> txMetadata )
    {
        checkBookmarks( bookmarks );
        return super.beginTransaction( loginContext, bookmarks, txTimeout, accessMode, txMetadata );
    }

    @Override
    public BoltTransaction beginPeriodicCommitTransaction( LoginContext loginContext, List<Bookmark> bookmarks, Duration txTimeout, AccessMode accessMode,
            Map<String,Object> txMetadata )
    {
        checkBookmarks( bookmarks );
        return super.beginPeriodicCommitTransaction( loginContext, bookmarks, txTimeout, accessMode, txMetadata );
    }

    @Override
    protected BoltResultHandle newBoltResultHandle( String statement, MapValue params, BoltQueryExecutor boltQueryExecutor )
    {
        return new BoltResultHandleV3( statement, params, boltQueryExecutor );
    }

    private void checkBookmarks( List<Bookmark> bookmarks )
    {
        if ( !bookmarks.isEmpty() && bookmarks.size() != 1 )
        {
            throw new IllegalArgumentException( "Expected zero or one bookmark. Received: " + bookmarks );
        }
    }

    private class BoltResultHandleV3 extends AbstractBoltResultHandle
    {
        BoltResultHandleV3( String statement, MapValue params, BoltQueryExecutor boltQueryExecutor )
        {
            super( statement, params, boltQueryExecutor );
        }

        @Override
        protected BoltResult newBoltResult( QueryExecution result,
                BoltAdapterSubscriber subscriber, Clock clock )
        {
            return new CypherAdapterStreamV3( result, subscriber, clock );
        }
    }
}
