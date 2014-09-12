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
package org.neo4j.kernel.ha;

import java.io.IOException;

import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;

/**
 * Commit process on slaves in HA. Transactions aren't committed here, but sent to the master, committed
 * there and streamed back.
 */
public class SlaveTransactionCommitProcess implements TransactionCommitProcess
{
    private final Master master;
    private final RequestContextFactory requestContextFactory;
    private final TransactionCommittingResponseUnpacker unpacker;

    public SlaveTransactionCommitProcess( Master master, RequestContextFactory requestContextFactory,
                                          TransactionCommittingResponseUnpacker unpacker )
    {
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.unpacker = unpacker;
    }

    @Override
    public long commit( LockGroup locks, TransactionRepresentation representation ) throws TransactionFailureException
    {
        try
        {
            return unpacker.unpackResponse( master.commit( requestContextFactory.newRequestContext(), representation ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
