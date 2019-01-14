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
package org.neo4j.kernel.impl.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionState;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionStateCloseException;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionStateHolder;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionStateManager;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionStateProvider;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.storageengine.api.StorageCommand;

public class KernelAuxTransactionStateManager implements AuxiliaryTransactionStateManager
{
    private volatile CopyOnWriteHashMap<Object,AuxiliaryTransactionStateProvider> providers;

    public KernelAuxTransactionStateManager()
    {
        providers = new CopyOnWriteHashMap<>();
    }

    @Override
    public void registerProvider( AuxiliaryTransactionStateProvider provider )
    {
        providers.put( provider.getIdentityKey(), provider );
    }

    @Override
    public void unregisterProvider( AuxiliaryTransactionStateProvider provider )
    {
        providers.remove( provider.getIdentityKey() );
    }

    @Override
    public AuxiliaryTransactionStateHolder openStateHolder()
    {
        return new AuxStateHolder( providers.snapshot() );
    }

    private static class AuxStateHolder implements AuxiliaryTransactionStateHolder, Function<Object,AuxiliaryTransactionState>
    {
        private final Map<Object,AuxiliaryTransactionStateProvider> providers;
        private final Map<Object,AuxiliaryTransactionState> openedStates;

        AuxStateHolder( Map<Object,AuxiliaryTransactionStateProvider> providers )
        {
            this.providers = providers;
            openedStates = new HashMap<>();
        }

        @Override
        public AuxiliaryTransactionState getState( Object providerIdentityKey )
        {
            return openedStates.computeIfAbsent( providerIdentityKey, this ); // Calls out to #apply(Object).
        }

        @Override
        public AuxiliaryTransactionState apply( Object providerIdentityKey )
        {
            AuxiliaryTransactionStateProvider provider = providers.get( providerIdentityKey );
            if ( provider != null )
            {
                return provider.createNewAuxiliaryTransactionState();
            }
            return null;
        }

        @Override
        public boolean hasChanges()
        {
            if ( openedStates.isEmpty() )
            {
                return false;
            }
            for ( AuxiliaryTransactionState state : openedStates.values() )
            {
                if ( state.hasChanges() )
                {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void extractCommands( Collection<StorageCommand> extractedCommands ) throws TransactionFailureException
        {
            for ( AuxiliaryTransactionState state : openedStates.values() )
            {
                if ( state.hasChanges() )
                {
                    state.extractCommands( extractedCommands );
                }
            }
        }

        @Override
        public void close() throws AuxiliaryTransactionStateCloseException
        {
            IOUtils.close( ( msg, cause ) -> new AuxiliaryTransactionStateCloseException( "Failure when closing auxiliary transaction state.", cause ),
                    openedStates.values() );
        }
    }
}
