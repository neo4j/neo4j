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
package org.neo4j.packstream.struct;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class EmptyStructRegistryTest {

    @Test
    void getInstanceShouldReturnSingleton() {
        var a = EmptyStructRegistry.getInstance();
        var b = EmptyStructRegistry.getInstance();

        assertThat(a).isSameAs(b);
    }

    @TestFactory
    Stream<DynamicTest> getReaderShouldReturnEmptyForAnyTag() {
        return IntStream.range(0, 5).mapToObj(i -> {
            var tag = i * 7;
            var length = i * 3;

            return dynamicTest(String.format("0x%02X (Length %d)", tag, length), () -> {
                var result = EmptyStructRegistry.getInstance().getReader(new StructHeader(length, (short) tag));

                assertThat(result).isEmpty();
            });
        });
    }

    @TestFactory
    Stream<DynamicTest> getWriterShouldReturnEmptyForAnyPayload() {
        return Stream.of(new Object(), (byte) 21, (short) 42, 84, (long) 8017, "Potato")
                .map(payload -> dynamicTest(payload.getClass().getName(), () -> {
                    var result = EmptyStructRegistry.getInstance().getWriter(payload);

                    assertThat(result).isEmpty();
                }));
    }
}
