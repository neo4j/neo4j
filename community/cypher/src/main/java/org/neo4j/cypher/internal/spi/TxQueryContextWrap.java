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
package org.neo4j.cypher.internal.spi;

import org.neo4j.kernel.api.TransactionContext;

public class TxQueryContextWrap {

    private final QueryContext queryCtx;
    private final TransactionContext tx;

    public TxQueryContextWrap(QueryContext queryCtx, TransactionContext tx) {
        this.queryCtx = queryCtx;
        this.tx = tx;
    }

    public QueryContext getQueryContext() {
        return queryCtx;
    }

    public void rollback() {
        queryCtx.close();

        // Rollback tx
        tx.finish();
    }

    public void commit() {
        queryCtx.close();

        // Commit tx
        tx.success();
        tx.finish();
    }
}
