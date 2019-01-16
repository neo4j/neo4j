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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RecordNodeCursorTest
{
    @Test
    void shouldConsiderHighestPossibleIdInUseInScan()
    {
        // given
        NodeStore nodeStore = mock( NodeStore.class );
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( 200L );
        when( nodeStore.getHighId() ).thenReturn( 20L );
        doAnswer( invocationOnMock ->
        {
            long id = invocationOnMock.getArgument( 0 );
            NodeRecord record = invocationOnMock.getArgument( 1 );
            record.setId( id );
            record.initialize( id == 200, 1L, false, 1L, 0L );
            return null;
        } ).when( nodeStore ).getRecordByCursor( anyLong(), any(), any(), any() );
        doAnswer( invocationOnMock ->
        {
            NodeRecord record = invocationOnMock.getArgument( 0 );
            record.setId( record.getId() + 1 );
            record.initialize( record.getId() == 200, 1L, false, 1L, 0L );
            return null;
        } ).when( nodeStore ).nextRecordByCursor( any(), any(), any() );
        RecordNodeCursor cursor = new RecordNodeCursor( nodeStore );

        // when
        cursor.scan();

        // then
        assertTrue( cursor.next() );
        assertEquals( 200, cursor.getId() );
        assertFalse( cursor.next() );
    }
}
