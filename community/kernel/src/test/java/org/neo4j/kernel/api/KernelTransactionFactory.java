/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api;

import org.neo4j.collection.pool.Pool;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.store.ProcedureCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.locking.NoOpLocks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocksFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KernelTransactionFactory
{
    static KernelTransaction kernelTransaction()
    {
        TransactionHeaderInformation headerInformation = new TransactionHeaderInformation( -1, -1, new byte[0] );
        TransactionHeaderInformationFactory headerInformationFactory = mock( TransactionHeaderInformationFactory.class );
        when( headerInformationFactory.create() ).thenReturn( headerInformation );

        return new KernelTransactionImplementation( mock( StatementOperationParts.class ),
                mock( SchemaWriteGuard.class ), null, null,
                null, mock( TransactionRecordState.class ),
                null, mock( NeoStores.class ),
                new TransactionHooks(), mock( ConstraintIndexCreator.class ), headerInformationFactory,
                mock( TransactionRepresentationCommitProcess.class ), mock( TransactionMonitor.class ),
                mock( StoreReadLayer.class ),
                mock( LegacyIndexTransactionState.class ),
                mock(Pool.class),
                mock( ConstraintSemantics.class ),
                Clock.SYSTEM_CLOCK,
                TransactionTracer.NULL,
                new ProcedureCache(),
                new SimpleStatementLocksFactory( new NoOpLocks() ),
                mock( NeoStoreTransactionContext.class ),
                false );
    }
}
