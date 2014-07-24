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
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;

public class SlaveTransactionCommitProcess extends TransactionRepresentationCommitProcess
{
    private final Master master;
    private final RequestContextFactory requestContextFactory;
    private final TransactionCommittingResponseUnpacker unpacker;

    public SlaveTransactionCommitProcess( Master master, RequestContextFactory requestContextFactory,
                                          LogicalTransactionStore logicalTransactionStore, KernelHealth kernelHealth,
                                          NeoStore neoStore, TransactionRepresentationStoreApplier storeApplier,
                                          TransactionCommittingResponseUnpacker unpacker, boolean recovery )
    {
        super( logicalTransactionStore, kernelHealth, neoStore, storeApplier, recovery );
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.unpacker = unpacker;
    }

    @Override
    public synchronized long commit( TransactionRepresentation representation ) throws TransactionFailureException
    {
        // TODO Oh my gawd, my eyes, fix this
        /*
         * The separation of the commit process to persist() and commit() is probably wrong, since
         * both the master and slave processes override the whole method. Revisit this and probably
         * undo the split
         */
        return persistTransaction( representation );
    }

    @Override
    public long persistTransaction( TransactionRepresentation representation ) throws TransactionFailureException
    {
        try
        {
            return unpacker.unpackResponse( master.commitSingleResourceTransaction( requestContextFactory.newRequestContext(), representation ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
