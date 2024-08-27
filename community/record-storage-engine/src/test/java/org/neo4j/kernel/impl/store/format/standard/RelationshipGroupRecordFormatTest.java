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
package org.neo4j.kernel.impl.store.format.standard;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

import java.util.Collection;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class RelationshipGroupRecordFormatTest {
    @Inject
    private RandomSupport random;

    @ParameterizedTest
    @MethodSource("formats")
    void shouldReadUnsignedRelationshipTypeId(RecordFormats formats) throws Exception {
        // GIVEN
        RecordFormat<RelationshipGroupRecord> format = formats.relationshipGroup();
        int recordSize = format.getRecordSize(NO_STORE_HEADER);
        try (PageCursor cursor = new StubPageCursor(1, recordSize * 10)) {
            int offset = 10;
            cursor.next();
            RelationshipGroupRecord group =
                    new RelationshipGroupRecord(2).initialize(true, Short.MAX_VALUE + offset, 1, 2, 3, 4, 5);
            group.setHasExternalDegreesOut(random.nextBoolean());
            group.setHasExternalDegreesIn(random.nextBoolean());
            group.setHasExternalDegreesLoop(random.nextBoolean());
            cursor.setOffset(offset);
            format.write(group, cursor, recordSize, cursor.getPagedFile().payloadSize() / recordSize);

            // WHEN
            RelationshipGroupRecord read = new RelationshipGroupRecord(group.getId());
            cursor.setOffset(offset);
            format.read(
                    read,
                    cursor,
                    NORMAL,
                    recordSize,
                    cursor.getPagedFile().payloadSize() / recordSize,
                    EmptyMemoryTracker.INSTANCE);

            // THEN
            assertEquals(group, read);
        }
    }

    private static Collection<RecordFormats> formats() {
        return asList(StandardV4_3.RECORD_FORMATS, StandardV5_0.RECORD_FORMATS);
    }
}
