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
package org.neo4j.kernel.api.txstate.auxiliary;

import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;

/**
 * Auxiliary transaction state can be used to attach "opaque" transaction state to a {@link KernelTransactionImplementation} from external sources.
 * <p>
 * Those external sources can put whatever transaction-specific data they need into the auxiliary transaction state, but it is up to those external sources,
 * to ensure that their transaction state is kept up to date. The {@link KernelTransactionImplementation#getTransactionDataRevision()} method can be helpful
 * for that purpose.
 * <p>
 * The {@link AuxiliaryTransactionStateProvider} is used as a factory of the {@link AuxiliaryTransactionState}, which is created when it is first requested
 * from the {@link TxStateHolder#auxiliaryTxState(Object)}, giving the {@link #getIdentityKey() identity key} of the particular auxiliary transaction state
 * provider.
 */
public interface AuxiliaryTransactionStateProvider
{
    /**
     * Return the <em>identity key</em> that is used to identify the provider, if {@link TxStateHolder#auxiliaryTxState(Object)} needs to have a new auxiliary
     * transaction state instance created.
     * <p>
     * Note that this object should have good equals and hashCode implementations, such that it cannot clash with keys from other providers.
     *
     * @return The key object used for identifying the auxiliary transaction state provider.
     */
    Object getIdentityKey();

    /**
     * @return a new instance of the {@link AuxiliaryTransactionState}.
     */
    AuxiliaryTransactionState createNewAuxiliaryTransactionState();
}
