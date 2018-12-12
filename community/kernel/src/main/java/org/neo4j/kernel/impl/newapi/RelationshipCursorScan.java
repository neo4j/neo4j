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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.iterator.LongIterator;

import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.storageengine.api.AllRelationshipsScan;

final class RelationshipCursorScan extends BaseCursorScan<RelationshipScanCursor,AllRelationshipsScan>
{

    RelationshipCursorScan( AllRelationshipsScan allRelationshipsScan, Read read )
    {
        super( allRelationshipsScan, read );
    }

    @Override
    long[] addedInTransaction()
    {
        return read.txState().addedAndRemovedRelationships().getAdded().toArray();
    }

    @Override
    boolean scanStore( RelationshipScanCursor cursor, int sizeHint, LongIterator addedItems )
    {
        return ((DefaultRelationshipScanCursor) cursor)
                .scanBatch( read, storageScan, sizeHint, addedItems, hasChanges );
    }
}
