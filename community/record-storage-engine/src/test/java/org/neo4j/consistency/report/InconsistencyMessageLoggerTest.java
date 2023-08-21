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
package org.neo4j.consistency.report;

import static java.lang.String.format;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.neo4j.consistency.RecordType;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.InternalLog;

class InconsistencyMessageLoggerTest {
    @Test
    void shouldUseRecordToStringFunctionForRecords() {
        // given
        InternalLog log = mock(InternalLog.class);
        InconsistencyMessageLogger logger = new InconsistencyMessageLogger(log, record -> record.hashCode() + " abc");

        // when
        NodeRecord record = new NodeRecord(0);
        logger.error(RecordType.NODE, record, "test message");
        logger.warning(RecordType.NODE, record, "test message");

        // then
        String expectedString = format("test message%n\t%s abc", record.hashCode());
        verify(log).error(expectedString);
        verify(log).warn(expectedString);
    }

    @Test
    void shouldUseRecordToStringFunctionForArgs() {
        // given
        InternalLog log = mock(InternalLog.class);
        InconsistencyMessageLogger logger = new InconsistencyMessageLogger(log, record -> record.hashCode() + " abc");

        // when
        NodeRecord record = new NodeRecord(0);
        NodeRecord argRecord = new NodeRecord(1);
        logger.error(RecordType.NODE, record, "test message", argRecord);
        logger.warning(RecordType.NODE, record, "test message", argRecord);

        // then
        String expectedString =
                format("test message%n\t%s abc%n\tInconsistent with: %s abc", record.hashCode(), argRecord.hashCode());
        verify(log).error(expectedString);
        verify(log).warn(expectedString);
    }
}
