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

import java.util.Collection;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.storageengine.api.StorageCommand;

/**
 * The container of, and facade to, the auxiliary transaction state that a transaction may hold.
 * <p>
 * Instances of this interface are obtained from the {@link AuxiliaryTransactionStateManager#openStateHolder()} method.
 */
public interface AuxiliaryTransactionStateHolder extends AutoCloseable
{
    /**
     * Get the auxiliary transaction state identified by the given {@link AuxiliaryTransactionStateProvider#getIdentityKey() provider identity key}.
     * <p>
     * If this transaction does not yet have transaction state from the given provider, then the provider is used to create an
     * {@link AuxiliaryTransactionState} instance, which is then cached for the remainder of the transaction.
     *
     * @param providerIdentityKey The {@link AuxiliaryTransactionStateProvider#getIdentityKey() provider identity key} that the desired provider is identified
     * by.
     * @return The transaction state from the given provider, either cached, or newly created.
     */
    AuxiliaryTransactionState getState( Object providerIdentityKey );

    /**
     * Used by the {@link KernelTransactionImplementation} to determine if the auxiliary transaction state may have any commands that needs to be extracted.
     * <p>
     * This would be the case if any of the internal auxiliary transaction state instances claims to
     * {@link AuxiliaryTransactionState#hasChanges() have changes}.
     *
     * @return {@code true} if calling {@link #extractCommands(Collection)} would yield commands.
     */
    boolean hasChanges();

    /**
     * Extract commands, if any, from the auxiliary transaction state instances.
     * <p>
     * This method delegates to the {@link AuxiliaryTransactionState#extractCommands(Collection)} method of all of the internal auxiliary transaction states
     * that claim to have changes ready for extraction.
     *
     * @param extractedCommands The collection to add the extracted commands to.
     * @throws TransactionFailureException If the transaction state wanted to produce commands, but is somehow unable to do so.
     */
    void extractCommands( Collection<StorageCommand> extractedCommands ) throws TransactionFailureException;

    /**
     * Close all of the internal {@link AuxiliaryTransactionState} instances, and release all of their resources.
     *
     * @throws AuxiliaryTransactionStateCloseException if something went wrong when closing the internal auxiliary transaction states.
     */
    @Override
    void close() throws AuxiliaryTransactionStateCloseException;
}
