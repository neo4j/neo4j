/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.kernel.impl.nioneo.store.StoreId;

public class SlaveImpl implements Slave
{
    private final Master master;
    private final RequestContextFactory requestContextFactory;
    private final StoreId storeId;
    private final HaXaDataSourceManager xaDsm;

    public SlaveImpl( StoreId storeId, Master master, RequestContextFactory requestContextFactory,
                      HaXaDataSourceManager xaDsm )
    {
        this.storeId = storeId;
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.xaDsm = xaDsm;
    }

    @Override
    public Response<Void> pullUpdates( String resource, long upToAndIncludingTxId )
    {
        // Pull updates from the master
        xaDsm.applyTransactions( master.pullUpdates( requestContextFactory.newRequestContext( 0 ) ), ServerUtil.NO_ACTION );
        return ServerUtil.packResponseWithoutTransactionStream( storeId, null );
    }

    @Override
    public int getServerId()
    {
        return 0;
    }
}
