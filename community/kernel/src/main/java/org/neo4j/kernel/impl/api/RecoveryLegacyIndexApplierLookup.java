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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.transaction.command.CommandHandler;

/**
 * Batches updates to a legacy index. Used during recovery.
 * After recovery has been completed {@link #close()} must be called.
 */
public class RecoveryLegacyIndexApplierLookup implements LegacyIndexApplierLookup, Closeable
{
    private final LegacyIndexApplierLookup lookup;
    private final Map<String,RecoveryCommandHandler> appliers = new HashMap<>();
    private final int batchSize;

    public RecoveryLegacyIndexApplierLookup( LegacyIndexApplierLookup lookup, int batchSize )
    {
        this.lookup = lookup;
        this.batchSize = batchSize;
    }

    @Override
    public CommandHandler newApplier( String name, boolean recovery )
    {
        RecoveryCommandHandler applier = appliers.get( name );
        if ( applier == null )
        {
            CommandHandler actualApplier = lookup.newApplier( name, recovery );
            appliers.put( name, applier = new RecoveryCommandHandler( name, actualApplier ) );
        }
        return applier;
    }

    /**
     * Called AFTER all transactions have been recovered, before forcing everything.
     */
    @Override
    public void close() throws IOException
    {
        for ( RecoveryCommandHandler applier : appliers.values() )
        {
            try ( RecoveryCommandHandler closeThisPlease = applier )
            {
                applier.applyForReal();
            }
        }
    }

    private class RecoveryCommandHandler extends CommandHandler.Delegator
    {
        private final String name;
        private int applyCount;
        private boolean applied;

        RecoveryCommandHandler( String name, CommandHandler applier )
        {
            super( applier );
            this.name = name;
        }

        @Override
        public void apply()
        {
            assert !applied;
            if ( ++applyCount % batchSize == 0 )
            {
                applyForReal();
            }
        }

        /**
         * Let close calls through since application of the changes is done in {@link #apply()},
         * and it's the batching of those that we're interested in.
         */
        @Override
        public void close()
        {
            if ( applied )
            {
                super.close();
            }
        }

        private void applyForReal()
        {
            super.apply();
            appliers.remove( name );
            applied = true;
        }
    }
}
