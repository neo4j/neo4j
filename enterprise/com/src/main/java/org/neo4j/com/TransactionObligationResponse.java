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

import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.kernel.impl.store.StoreId;

/**
 * {@link Response} that carries transaction obligation as a side-effect.
 *
 * @see TransactionObligationFulfiller
 */
public class TransactionObligationResponse<T> extends Response<T>
{
    static final byte RESPONSE_TYPE = -1;

    static final Response<Void> EMPTY_RESPONSE = new TransactionObligationResponse<>( null, StoreId.DEFAULT,
            -1, ResourceReleaser.NO_OP );

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
