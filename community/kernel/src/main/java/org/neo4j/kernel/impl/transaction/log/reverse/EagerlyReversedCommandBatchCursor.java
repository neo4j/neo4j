/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.reverse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

/**
 * Eagerly exhausts a {@link CommandBatchCursor} and allows moving through it in reverse order.
 * The idea is that this should only be done for a subset of a bigger transaction log stream, typically
 * for one log file.
 *
 * For reversing a transaction log consisting of multiple log files {@link ReversedMultiFileCommandBatchCursor}
 * should be used (it will use this class internally though).
 *
 * @see ReversedMultiFileCommandBatchCursor
 */
public class EagerlyReversedCommandBatchCursor implements CommandBatchCursor {
    private final List<CommittedCommandBatch> batches = new ArrayList<>();
    private final CommandBatchCursor cursor;
    private int indexToReturn;

    public EagerlyReversedCommandBatchCursor(CommandBatchCursor cursor) throws IOException {
        this.cursor = cursor;
        while (cursor.next()) {
            batches.add(cursor.get());
        }
        this.indexToReturn = batches.size();
    }

    @Override
    public boolean next() {
        if (indexToReturn > 0) {
            indexToReturn--;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        cursor.close();
    }

    @Override
    public CommittedCommandBatch get() {
        return batches.get(indexToReturn);
    }

    @Override
    public LogPosition position() {
        throw new UnsupportedOperationException("Should not be called");
    }

    public static CommandBatchCursor eagerlyReverse(CommandBatchCursor cursor) throws IOException {
        return new EagerlyReversedCommandBatchCursor(cursor);
    }
}
