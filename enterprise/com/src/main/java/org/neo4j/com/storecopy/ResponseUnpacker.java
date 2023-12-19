/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.com.storecopy;

import org.neo4j.com.Response;

public interface ResponseUnpacker
{
    /**
     * @param txHandler for getting an insight into which transactions gets applied.
     */
    void unpackResponse( Response<?> response, TxHandler txHandler ) throws Exception;

    ResponseUnpacker NO_OP_RESPONSE_UNPACKER = ( response, txHandler ) ->
    {
        /* Do nothing */
    };

    interface TxHandler
    {
        TxHandler NO_OP_TX_HANDLER = transactionId ->
        {
            /* Do nothing */
        };

        void accept( long transactionId );
    }
}
