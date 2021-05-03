/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.log;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;

/**
 * Represents a commitment that invoking {@link TransactionAppender#append(TransactionToApply, LogAppendEvent)}
 * means. As a transaction is carried through the {@link TransactionCommitProcess} this commitment is updated
 * when {@link #publishAsCommitted(CursorContext)}  committed} (which happens when appending to log), but also
 * when {@link #publishAsClosed(CursorContext)} closing}.
 */
public interface Commitment
{
    Commitment NO_COMMITMENT = new Commitment()
    {
        @Override
        public void publishAsCommitted( CursorContext cursorContext )
        {
        }

        @Override
        public void publishAsClosed( CursorContext cursorContext )
        {
        }

        @Override
        public boolean markedAsCommitted()
        {
            return false;
        }

    };

    /**
     * Marks the transaction as committed and makes this fact public.
     */
    void publishAsCommitted( CursorContext cursorContext );

    /**
     * Marks the transaction as closed and makes this fact public.
     */
    void publishAsClosed( CursorContext cursorContext );

    /**
     * @return whether or not {@link #publishAsCommitted(CursorContext)} have been called.
     */
    boolean markedAsCommitted();
}
