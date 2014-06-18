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
package org.neo4j.com;

import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;

public class Response<T> implements AutoCloseable
{
    private final T response;
    private final StoreId storeId;
    private final ResourceReleaser releaser;
    private final Iterable<CommittedTransactionRepresentation> txs;

    public Response( T response, StoreId storeId,
                     Iterable<CommittedTransactionRepresentation> txs, ResourceReleaser releaser )
    {
        this.storeId = storeId;
        this.response = response;
        this.txs = txs;
        this.releaser = releaser;
    }

    public T response() throws ServerFailureException
    {
        return response;
    }

    public StoreId getStoreId()
    {
        return storeId;
    }

    @Override
    public void close()
    {
        releaser.release();
    }

    public static <T> Response<T> empty()
    {
        return new Response<T>( null, new StoreId( -1, -1 ), null, ResourceReleaser.NO_OP );
    }

    public Iterable<CommittedTransactionRepresentation> getTxs()
    {
        return txs;
    }
}
