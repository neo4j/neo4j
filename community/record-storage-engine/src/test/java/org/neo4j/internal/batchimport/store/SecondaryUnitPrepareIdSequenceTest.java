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
package org.neo4j.internal.batchimport.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.id.IdSequence;

class SecondaryUnitPrepareIdSequenceTest {
    @Test
    void shouldReturnIdImmediatelyAfterRecordId() {
        // given
        PrepareIdSequence idSequence = new SecondaryUnitPrepareIdSequence();
        IdSequence actual = mock(IdSequence.class);

        // when
        long recordId = 10;
        IdSequence prepared = idSequence.apply(actual).apply(recordId);
        long nextRecordId = prepared.nextId(NULL_CONTEXT);

        // then
        assertEquals(10 + 1, nextRecordId);
        verifyNoMoreInteractions(actual);
    }

    @Test
    void shouldReturnIdImmediatelyAfterRecordIdOnlyOnce() {
        // given
        PrepareIdSequence idSequence = new SecondaryUnitPrepareIdSequence();
        IdSequence actual = mock(IdSequence.class);

        // when
        long recordId = 10;
        IdSequence prepared = idSequence.apply(actual).apply(recordId);
        long nextRecordId = prepared.nextId(NULL_CONTEXT);
        assertEquals(10 + 1, nextRecordId);
        verifyNoMoreInteractions(actual);
        try {
            prepared.nextId(NULL_CONTEXT);
            fail("Should've failed");
        } catch (IllegalStateException e) { // good
        }

        // and when
        recordId = 20;
        prepared = idSequence.apply(actual).apply(recordId);
        nextRecordId = prepared.nextId(NULL_CONTEXT);

        // then
        assertEquals(20 + 1, nextRecordId);
        verifyNoMoreInteractions(actual);
    }
}
