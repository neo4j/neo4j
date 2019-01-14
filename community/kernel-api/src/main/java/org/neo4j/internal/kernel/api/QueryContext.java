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
package org.neo4j.internal.kernel.api;

import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

/**
 * Represents the specific transactional context in which an index query is executed.
 *
 * Some index implementations, such as the fulltext schema indexes, prefer to take the transaction state into account themselves, rather than relying on
 * the index cursor implementations to do the transaction state filtering.
 */
public interface QueryContext
{
    QueryContext NULL_CONTEXT = new QueryContext()
    {
        @Override
        public Read getRead()
        {
            throw new UnsupportedOperationException( "The NULL_CONTEXT QueryContext has no Read instance." );
        }

        @Override
        public CursorFactory cursors()
        {
            throw new UnsupportedOperationException( "The NULL_CONTEXT QueryContext has no CursorFactory instance." );
        }

        @Override
        public ReadableTransactionState getTransactionStateOrNull()
        {
            return null;
        }
    };

    /**
     * Get the {@link Read} interface to the kernel, which is a view of the store as seen from within the current transaction.
     *
     * This means that store reads through this interface are filtered through the transaction state, if any, of the current transaction.
     * @return a {@link Read} instance in the context of the current transaction.
     */
    Read getRead();

    /**
     * @return the {@link CursorFactory} associated with the context of this index query.
     */
    CursorFactory cursors();

    /**
     * @return the {@link ReadableTransactionState} of the current transaction, if there is any, otherwise {@code null}. Note that this will <em>not</em>
     * create any transaction state, if there is none associated with the current transaction.
     */
    ReadableTransactionState getTransactionStateOrNull();
}
