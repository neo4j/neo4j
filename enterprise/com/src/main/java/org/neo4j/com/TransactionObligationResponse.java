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

import java.io.IOException;

import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.kernel.impl.store.StoreId;

/**
 * {@link Response} that carries transaction obligation as a side-effect.
 *
 * @see TransactionObligationFulfiller
 */
public class TransactionObligationResponse<T> extends Response<T>
{
    public static final byte RESPONSE_TYPE = -1;

    private final long obligationTxId;

    public TransactionObligationResponse( T response, StoreId storeId, long obligationTxId, ResourceReleaser releaser )
    {
        super( response, storeId, releaser );
        this.obligationTxId = obligationTxId;
    }

    @Override
    public void accept( Response.Handler handler ) throws IOException
    {
        handler.obligation( obligationTxId );
    }

    @Override
    public boolean hasTransactionsToBeApplied()
    {
        return false;
    }
}
