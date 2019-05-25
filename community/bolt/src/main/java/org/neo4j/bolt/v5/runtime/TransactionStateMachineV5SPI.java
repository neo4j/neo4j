/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v5.runtime;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.blob.BoltTransactionListener;
import org.neo4j.bolt.v3.runtime.TransactionStateMachineV3SPI;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;

public class TransactionStateMachineV5SPI extends TransactionStateMachineV3SPI
{
    public TransactionStateMachineV5SPI( GraphDatabaseAPI db, BoltChannel boltChannel, Duration txAwaitDuration,
                                        Clock clock )
    {
        super( db, boltChannel, txAwaitDuration, clock );
    }

    @Override
    protected InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext,
                                                   Duration txTimeout, Map<String,Object> txMetadata )
    {
        InternalTransaction tx = super.beginTransaction( type, loginContext, txTimeout, txMetadata );
        BoltTransactionListener.onTransactionCreate( tx );
        return tx;
    }
}
