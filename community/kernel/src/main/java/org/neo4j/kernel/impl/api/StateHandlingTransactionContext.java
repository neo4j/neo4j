/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.impl.api.state.OldTxStateBridgeImpl;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.core.TransactionState;

public class StateHandlingTransactionContext extends DelegatingTransactionContext
{
    private final PersistenceCache persistenceCache;
    private final TxState state;
    private final SchemaCache schemaCache;
    
    /**
     * This is tech debt and coupled with {@link OldBridgingTransactionStateStatementContext}
     */
    @Deprecated
    private final TransactionState oldTransactionState;

    public StateHandlingTransactionContext( TransactionContext actual, PersistenceCache persistenceCache,
            TransactionState oldTransactionState, SchemaCache schemaCache )
    {
        super(actual);
        this.persistenceCache = persistenceCache;
        this.oldTransactionState = oldTransactionState;
        this.schemaCache = schemaCache;
        this.state = new TxState(new OldTxStateBridgeImpl( oldTransactionState ));
    }

    @Override
    public StatementContext newStatementContext()
    {
        // Store stuff
        StatementContext result = super.newStatementContext();
        // + Caching
        result = new CachingStatementContext( result, persistenceCache, schemaCache );
        // + Transaction-local state awareness
        result = new TransactionStateAwareStatementContext( result, state );
        // + Old transaction state bridge
        result = new OldBridgingTransactionStateStatementContext( result, oldTransactionState );

        // done
        return result;
    }

    @Override
    public void finish()
    {
        // - Ensure transaction is committed to disk at this point
        super.finish();
        // - commit schema changes from tx state to the schema cache
        //   (this is instead currently done via WriteTransaction, so that even externally applied
        //    transactions updates the schema cache)
//        schemaCache.apply( state );
        // - commit changes from tx state to the cache
        // TODO: This should *not* be done here, it should be done as part of transaction application (eg WriteTransaction)
        persistenceCache.apply( state );
    }
}
