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
package org.neo4j.com.storecopy;

import java.io.IOException;

import org.neo4j.com.AccumulatorVisitor;
import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchTransactionException;

import static org.neo4j.kernel.impl.util.Cursors.exhaustAndClose;

public class ResponsePacker
{
    private final LogicalTransactionStore transactionStore;
    private final GraphDatabaseAPI db;

    public ResponsePacker( LogicalTransactionStore transactionStore, GraphDatabaseAPI db )
    {
        this.transactionStore = transactionStore;
        this.db = db; // just so that we can get the store ID at a later point. It's probably not available right now
    }

    public <T> Response<T> packResponse( RequestContext context, T response ) throws IOException
    {
        return packResponse( context, response, Predicates.<CommittedTransactionRepresentation>TRUE() );
    }

    public <T> Response<T> packResponse( RequestContext context, T response,
            Predicate<CommittedTransactionRepresentation> filter ) throws IOException
    {
        AccumulatorVisitor<CommittedTransactionRepresentation> accumulator = new AccumulatorVisitor<>( filter );
        try
        {
            exhaustAndClose( transactionStore.getCursor( context.lastAppliedTransaction() + 1, accumulator ) );
        }
        catch ( NoSuchTransactionException e )
        {   // OK, so there were no transactions to pack
            // TODO do this w/o exception being thrown?
        }
        Iterable<CommittedTransactionRepresentation> txs = accumulator.getAccumulator();
        return new Response<>( response, db.storeId(), txs, ResourceReleaser.NO_OP );
    }
}
