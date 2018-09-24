/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import java.util.IdentityHashMap;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.txstate.aux.AuxiliaryTransactionState;
import org.neo4j.kernel.api.txstate.aux.AuxiliaryTransactionStateCloseException;
import org.neo4j.kernel.api.txstate.aux.AuxiliaryTransactionStateHolder;
import org.neo4j.kernel.api.txstate.aux.AuxiliaryTransactionStateManager;
import org.neo4j.kernel.api.txstate.aux.AuxiliaryTransactionStateProvider;
import org.neo4j.storageengine.api.StorageCommand;

public class KernelAuxTransactionStateManager implements AuxiliaryTransactionStateManager
{
    private volatile IdentityHashMap<Object, AuxiliaryTransactionStateProvider> providers;

    public KernelAuxTransactionStateManager()
    {
        providers = new IdentityHashMap<>();
    }

    @Override
    public void registerProvider( AuxiliaryTransactionStateProvider provider )
    {
        Object key = provider.getIdentityKey();
        synchronized ( this )
        {
            if ( !providers.containsKey( key ) )
            {
                IdentityHashMap<Object, AuxiliaryTransactionStateProvider> copy = new IdentityHashMap<>( providers );
                copy.put( key, provider );
                providers = copy;
            }
        }
    }

    @Override
    public void unregisterProvider( AuxiliaryTransactionStateProvider provider )
    {
        Object key = provider.getIdentityKey();
        synchronized ( this )
        {
            if ( providers.containsKey( key ) )
            {
                IdentityHashMap<Object, AuxiliaryTransactionStateProvider> copy = new IdentityHashMap<>( providers );
                copy.remove( key );
                providers = copy;
            }
        }
    }

    @Override
    public AuxiliaryTransactionStateHolder openStateHolder()
    {
        return new AuxStateHolder( providers );
    }

    private static class AuxStateHolder implements AuxiliaryTransactionStateHolder
    {
        private final IdentityHashMap<Object, AuxiliaryTransactionStateProvider> providers;
        private final IdentityHashMap<Object, AuxiliaryTransactionState> openedStates;

        AuxStateHolder( IdentityHashMap<Object,AuxiliaryTransactionStateProvider> providers )
        {
             this.providers = providers;
            openedStates = new IdentityHashMap<>();
        }

        @Override
        public AuxiliaryTransactionState getState( Object providerIdentityKey )
        {
            AuxiliaryTransactionState state = openedStates.get( providerIdentityKey );
            if ( state == null )
            {
                AuxiliaryTransactionStateProvider provider = providers.get( providerIdentityKey );
                if ( provider != null )
                {
                    state = provider.createNewAuxiliaryTransactionState();
                    openedStates.put( providerIdentityKey, state );
                }
            }
            return state;
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
        public boolean hasChanges( String providerIdentityKey )
        {
            AuxiliaryTransactionState state = openedStates.get( providerIdentityKey );
            return state != null && state.hasChanges();
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
            AuxiliaryTransactionStateCloseException exception = null;
            for ( AuxiliaryTransactionState state : openedStates.values() )
            {
                try
                {
                    state.close();
                }
                catch ( Exception e )
                {
                    if ( exception == null )
                    {
                        exception = new AuxiliaryTransactionStateCloseException( "Failure when closing auxiliary transaction state.", e );
                    }
                    else
                    {
                        exception.addSuppressed( e );
                    }
                }
            }
        }
    }
}
