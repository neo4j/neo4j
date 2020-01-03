/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

/**
 * The auxiliary transaction state manager keeps track of what auxiliary transaction state providers are available, and is responsible for creating
 * {@link AuxiliaryTransactionStateHolder} instances, which are the per-transaction containers of auxiliary transaction state.
 */
public interface AuxiliaryTransactionStateManager
{
    /**
     * Register a new {@link AuxiliaryTransactionStateProvider}.
     * <p>
     * This method is thread-safe. Only {@link AuxiliaryTransactionStateHolder} instances that are opened after the completion of a provider registration,
     * will have that provider available.
     *
     * @param provider The provider to register.
     */
    void registerProvider( AuxiliaryTransactionStateProvider provider );

    /**
     * Unregister the given {@link AuxiliaryTransactionStateProvider}.
     * <p>
     * This method is thread-safe. The transaction state provider will still be referenced by existing {@link AuxiliaryTransactionStateHolder} instances,
     * but will not be available to holders that are opened after the unregistration completes.
     *
     * @param provider The provider to unregister.
     */
    void unregisterProvider( AuxiliaryTransactionStateProvider provider );

    /**
     * Open a new {@link AuxiliaryTransactionStateHolder} for a transaction.
     *
     * @return A new auxiliary transaction state holder.
     */
    AuxiliaryTransactionStateHolder openStateHolder();
}
