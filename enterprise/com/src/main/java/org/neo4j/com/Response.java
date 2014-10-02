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

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;

public abstract class Response<T> implements AutoCloseable
{
    private final T response;
    private final StoreId storeId;
    private final ResourceReleaser releaser;

    public Response( T response, StoreId storeId, ResourceReleaser releaser )
    {
        this.storeId = storeId;
        this.response = response;
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

    @SuppressWarnings( "unchecked" )
    public static <T> Response<T> empty()
    {
        return (Response<T>) EMPTY;
    }

    public abstract void accept( Handler handler ) throws IOException;

    /**
     * Handler of the transaction data part of a response. Callbacks for whether to await or apply
     * certain transactions.
     */
    public interface Handler
    {
        void obligation( long txId ) throws IOException;

        /**
         * Transaction stream is starting, containing the following data sources.
         * Only called if there are at least one transaction in the coming transaction stream.
         */
        Visitor<CommittedTransactionRepresentation,IOException> transactions();
    }

    public static final Response<Void> EMPTY = new TransactionObligationResponse<Void>( null, StoreId.DEFAULT,
            -1, ResourceReleaser.NO_OP );
}
