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
package org.neo4j.internal.kernel.api.helpers;

import java.util.List;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.ExecutionContext;

public record PartitionedMultiScan<INNER extends Cursor, OUTER extends Cursor>(List<PartitionedScan<INNER>> scans)
        implements PartitionedScan<OUTER> {

    @Override
    public int getNumberOfPartitions() {
        return scans.size() > 0 ? scans.get(0).getNumberOfPartitions() : 0;
    }

    @Override
    public boolean reservePartition(OUTER cursor, CursorContext cursorContext, AccessMode accessMode) {
        throw new UnsupportedOperationException("Do not call");
    }

    @Override
    public boolean reservePartition(OUTER cursor, ExecutionContext executionContext) {
        throw new UnsupportedOperationException("Do not call");
    }

    public void reservePartition(INNER[] cursors, ExecutionContext executionContext) {
        assert cursors.length == scans.size();
        synchronized (scans) {
            for (int i = 0; i < cursors.length; i++) {
                scans.get(i).reservePartition(cursors[i], executionContext);
            }
        }
    }
}
