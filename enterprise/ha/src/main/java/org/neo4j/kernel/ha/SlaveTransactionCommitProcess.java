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
package org.neo4j.kernel.ha;

import java.io.IOException;

import org.neo4j.com.ComException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;

/**
 * Commit process on slaves in HA. Transactions aren't committed here, but sent to the master, committed
 * there and streamed back. Look at {@link org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker}
 */
public class SlaveTransactionCommitProcess implements TransactionCommitProcess
{
    private final Master master;
    private final RequestContextFactory requestContextFactory;

    public SlaveTransactionCommitProcess( Master master, RequestContextFactory requestContextFactory )
    {
        this.master = master;
        this.requestContextFactory = requestContextFactory;
    }

    @Override
    public long commit( TransactionRepresentation representation, LockGroup locks, CommitEvent commitEvent,
                        TransactionApplicationMode mode ) throws TransactionFailureException
    {
        try
        {
            RequestContext context = requestContextFactory.newRequestContext( representation.getLockSessionId() );
            try ( Response<Long> response = master.commit( context, representation ) )
            {
                return response.response();
            }
        }
        catch ( IOException e )
        {
            throw new TransactionFailureException(
                    Status.Transaction.CouldNotCommit, e, "Could not commit transaction on the master" );
        }
        catch ( ComException e )
        {
            throw new TransientTransactionFailureException(
                    "Cannot commit this transaction on the master. " +
                    "The master is either down, or we have network connectivity problems.", e );
        }
    }
}
