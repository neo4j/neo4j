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
package org.neo4j.com.storecopy;

import java.io.IOException;

import org.neo4j.com.Response;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;

public interface ResponseUnpacker
{
    /**
     * @param txHandler for getting an insight into which transactions gets applied.
     */
    void unpackResponse( Response<?> response, TxHandler txHandler ) throws IOException;

    public static final ResponseUnpacker NO_OP_RESPONSE_UNPACKER = new ResponseUnpacker()
    {
        @Override
        public void unpackResponse( Response<?> response, TxHandler txHandler ) throws IOException
        {
            txHandler.done();
        }
    };

    public interface TxHandler
    {
        void accept( CommittedTransactionRepresentation tx );

        void done();
    }

    public static final TxHandler NO_OP_TX_HANDLER = new TxHandler()
    {
        @Override
        public void accept( CommittedTransactionRepresentation tx )
        {   // Do nothing
        }

        @Override
        public void done()
        {   // Do nothing
        }
    };
}
