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
package org.neo4j.com.storecopy;

import java.io.IOException;

import org.neo4j.com.Response;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionAppender;

public class TransactionCommittingResponseUnpacker extends ResponseUnpacker.Adapter
{
    private final TransactionAppender appender;
    private final TransactionRepresentationStoreApplier storeApplier;

    public TransactionCommittingResponseUnpacker( TransactionAppender appender,
            TransactionRepresentationStoreApplier storeApplier )
    {
        this.appender = appender;
        this.storeApplier = storeApplier;
    }

    @Override
    public <T> T unpackResponse( Response<T> response, TxHandler handler ) throws IOException
    {
        for ( CommittedTransactionRepresentation transaction : response.getTxs() )
        {
            // TODO why do we synchronize here, read all about it at
            // TransactionAppender#append(CommittedTransactionRepresentation)
            synchronized ( appender )
            {
                if ( appender.append( transaction ) )
                {
                    storeApplier.apply( transaction.getTransactionRepresentation(),
                            transaction.getCommitEntry().getTxId(), true ); // TODO recovery=true needed?
                    handler.accept( transaction );
                }
            }
        }
        return response.response();
    }
}
