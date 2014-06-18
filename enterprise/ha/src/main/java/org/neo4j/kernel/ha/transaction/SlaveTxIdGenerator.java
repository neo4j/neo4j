/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.ha.transaction;

import org.neo4j.com.ComException;
import org.neo4j.com.Response;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;

public class SlaveTxIdGenerator implements TxIdGenerator
{
    private final int serverId;
    private final Master master;
    private final int masterId;
    private final RequestContextFactory requestContextFactory;

    public SlaveTxIdGenerator( int serverId, Master master, int masterId, RequestContextFactory requestContextFactory )
    {
        this.serverId = serverId;
        this.masterId = masterId;
        this.requestContextFactory = requestContextFactory;
        this.master = master;
    }

    @Override
    public long generate( TransactionRepresentation transactionRepresentation )
    {
        try
        {
            // For the first resource to commit against, make sure the master tx is initialized. This is sub
            // optimal to do here, since we are under a synchronized block, but writing to master from slaves
            // is discouraged in any case. For details of the background for this call, see TransactionState
            // and its isRemoteInitialized method.

            // TODO 2.2-future We need to decide on how the hell we will transfer transactions over
//            Response<Long> response = master.commitSingleResourceTransaction(
//                    requestContextFactory.newRequestContext(), transactionRepresentation );
            // TODO 2.2-future find a way to apply transactions
//            xaDsm.applyTransactions( response );
            Response<Long> response = null;
            return response.response().longValue();
        }
        catch ( ComException e )
        {
            throw new RuntimeException( e );
        }
    }
}
