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
package org.neo4j.kernel.impl.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.kernel.impl.store.format.standard.PropertyRecordFormat;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
class PropertyValueRecordSizeCalculatorTest {
    private static final int PROPERTY_RECORD_SIZE = PropertyRecordFormat.RECORD_SIZE;
    private static final int DYNAMIC_RECORD_SIZE = 120;

    @Inject
    private RandomSupport random;

    @Test
    void shouldIncludePropertyRecordSize() {
        // given
        PropertyValueRecordSizeCalculator calculator = newCalculator();

        // when
        int size = calculator.calculateSize(new Value[] {Values.of(10)}, NULL_CONTEXT, INSTANCE);

        // then
        assertEquals(PropertyRecordFormat.RECORD_SIZE, size);
    }

    @Test
    void shouldIncludeDynamicRecordSizes() {
        // given
        PropertyValueRecordSizeCalculator calculator = newCalculator();

        // when
        int size = calculator.calculateSize(
                new Value[] {Values.of(string(80)), Values.of(new String[] {string(150)})}, NULL_CONTEXT, INSTANCE);

        // then
        assertEquals(PROPERTY_RECORD_SIZE + DYNAMIC_RECORD_SIZE + DYNAMIC_RECORD_SIZE * 2, size);
    }

    @Test
    void shouldSpanMultiplePropertyRecords() {
        // given
        PropertyValueRecordSizeCalculator calculator = newCalculator();

        // when
        int size = calculator.calculateSize(
                new Value[] {
                    Values.of(10), // 1 block  go to record 1
                    Values.of("test"), // 1 block
                    Values.of((byte) 5), // 1 block
                    Values.of(string(80)), // 1 block
                    Values.of("a bit longer short string"), // 3 blocks go to record 2
                    Values.of(1234567890123456789L), // 2 blocks go to record 3
                    Values.of(5), // 1 block
                    Values.of("value") // 1 block
                },
                NULL_CONTEXT,
                INSTANCE);

        // then
        assertEquals(PROPERTY_RECORD_SIZE * 3 + DYNAMIC_RECORD_SIZE, size);
    }

    private String string(int length) {
        return random.nextAlphaNumericString(length, length);
    }

    private static PropertyValueRecordSizeCalculator newCalculator() {
        return new PropertyValueRecordSizeCalculator(
                PROPERTY_RECORD_SIZE,
                DYNAMIC_RECORD_SIZE,
                DYNAMIC_RECORD_SIZE - 10,
                DYNAMIC_RECORD_SIZE,
                DYNAMIC_RECORD_SIZE - 10);
    }
}
