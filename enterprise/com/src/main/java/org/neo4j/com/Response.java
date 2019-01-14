/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.com;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;

/**
 * In response to a {@link Client#sendRequest(RequestType, RequestContext, Serializer, Deserializer) request}
 * which contains a response value (T), and optionally some sort of side-effect,
 * like {@link TransactionStreamResponse transaction stream} or {@link TransactionObligationResponse transaction oglibation}.
 */
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
        return (Response<T>) TransactionObligationResponse.EMPTY_RESPONSE;
    }

    public abstract void accept( Handler handler ) throws Exception;

    /**
     * @return {@code true} if this response has transactions to be applied as part of unpacking it,
     * otherwise {@code false}.
     */
    public abstract boolean hasTransactionsToBeApplied();

    /**
     * Handler of the transaction data part of a response. Callbacks for whether to await or apply
     * certain transactions.
     */
    public interface Handler
    {
        /**
         * Called for responses that handle {@link TransactionObligationResponse transaction obligations}
         * after the obligation transaction id has been deserialized.
         * @param txId the obligation transaction id that must be fulfilled.
         * @throws IOException if there were any problems fulfilling that obligation.
         */
        void obligation( long txId ) throws IOException;

        /**
         * @return a {@link Visitor} which will {@link Visitor#visit(Object) receive} calls about transactions.
         */
        Visitor<CommittedTransactionRepresentation,Exception> transactions();
    }
}
