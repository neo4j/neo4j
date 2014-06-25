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

    public SlaveTransactionCommitProcess( Master master, RequestContextFactory requestContextFactory,
                                          LogicalTransactionStore logicalTransactionSTore, KernelHealth kernelHealth,
                                          NeoStore neoStore, TransactionRepresentationStoreApplier storeApplier,
                                          boolean recovery )
    {
        super( logicalTransactionSTore, kernelHealth, neoStore, storeApplier, recovery );
        this.master = master;
        this.requestContextFactory = requestContextFactory;
    }

    @Override
    public long persistTransaction( TransactionRepresentation representation ) throws TransactionFailureException
    {
        return master.commitSingleResourceTransaction( requestContextFactory.newRequestContext(), representation ).response();
    }
}
