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
package org.neo4j.internal.recordstorage;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import org.neo4j.io.pagecache.tracing.cursor.CursorContext;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.pagecache.tracing.cursor.CursorContext.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class RecordChangesTest
{
    private final RecordAccess.Loader<NodeRecord, Object> loader = new RecordAccess.Loader<>()
    {
        @Override
        public NodeRecord newUnused( long id, Object additionalData )
        {
            return new NodeRecord( id );
        }

        @Override
        public NodeRecord load( long id, Object additional, RecordLoad load, CursorContext cursorContext )
        {
            return new NodeRecord( id );
        }

        @Override
        public void ensureHeavy( NodeRecord o, CursorContext cursorContext )
        {

        }

        @Override
        public NodeRecord copy( NodeRecord o )
        {
            return o;
        }
    };

    @Test
    void shouldCountChanges()
    {
        // Given
        RecordChanges<NodeRecord, Object> change = new RecordChanges<>( loader, new MutableInt(), INSTANCE, RecordAccess.LoadMonitor.NULL_MONITOR );

        // When
        change.getOrLoad( 1, null, NULL ).forChangingData();
        change.getOrLoad( 1, null, NULL ).forChangingData();
        change.getOrLoad( 2, null, NULL ).forChangingData();
        change.getOrLoad( 3, null, NULL ).forReadingData();

        // Then
        assertThat( change.changeSize() ).isEqualTo( 2 );
    }
}
