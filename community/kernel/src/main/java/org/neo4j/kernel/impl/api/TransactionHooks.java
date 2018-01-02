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
package org.neo4j.kernel.impl.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.TransactionHook;
import org.neo4j.kernel.api.TransactionHook.Outcome;
import org.neo4j.kernel.api.exceptions.TransactionHookException;
import org.neo4j.kernel.api.txstate.ReadableTxState;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;

public class TransactionHooks
{
    protected final Set<TransactionHook> hooks = new CopyOnWriteArraySet<>();

    public void register( TransactionHook hook )
    {
        hooks.add( hook );
    }

    public void unregister( TransactionHook hook )
    {
        hooks.remove( hook );
    }

    public TransactionHooksState beforeCommit( ReadableTxState state, KernelTransaction tx, StoreReadLayer storeReadLayer )
    {
        if ( hooks.size() == 0 )
        {
            return null;
        }

        TransactionHooksState hookState = new TransactionHooksState();
        for ( TransactionHook hook : hooks )
        {
            hookState.add( hook, hook.beforeCommit( state, tx, storeReadLayer ) );
        }
        return hookState;
    }

    @SuppressWarnings( "unchecked" )
    public void afterCommit( ReadableTxState state, KernelTransaction tx, TransactionHooksState hooksState )
    {
        if ( hooksState == null )
        {
            return;
        }
        for ( Pair<TransactionHook, Outcome> hookAndOutcome : hooksState.hooksWithOutcome() )
        {
            hookAndOutcome.first().afterCommit( state, tx, hookAndOutcome.other() );
        }
    }

    @SuppressWarnings( "unchecked" )
    public void afterRollback( ReadableTxState state, KernelTransaction tx, TransactionHooksState hooksState )
    {
        if(hooksState == null)
        {
            return;
        }
        for ( Pair<TransactionHook, Outcome> hookAndOutcome : hooksState.hooksWithOutcome() )
        {
            hookAndOutcome.first().afterRollback( state, tx, hookAndOutcome.other() );
        }
    }

    public static class TransactionHooksState
    {
        private final List<Pair<TransactionHook, Outcome>> hooksWithAttachment = new ArrayList<>();
        private TransactionHookException failure;

        public void add( TransactionHook hook, Outcome outcome )
        {
            hooksWithAttachment.add( Pair.of( hook, outcome ) );
            if(outcome != null && !outcome.isSuccessful())
            {
                failure = new TransactionHookException(outcome.failure(), "Transaction handler failed.");
            }
        }

        public Iterable<Pair<TransactionHook, Outcome>> hooksWithOutcome()
        {
            return hooksWithAttachment;
        }

        public boolean failed()
        {
            return failure != null;
        }

        public TransactionHookException failure()
        {
            return failure;
        }
    }
}
