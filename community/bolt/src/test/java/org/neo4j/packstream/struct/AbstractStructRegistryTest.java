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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.HashMap;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbstractStructRegistryTest {
    private StructRegistryImpl registry;

    @BeforeEach
    void prepareRegistry() {
        this.registry = new StructRegistryImpl();
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void getReaderShouldReturnRegisteredReader() {
        var reader = mock(StructReader.class);
        this.registry.registerReader((short) 0x42, reader);

        var result = this.registry.getReader(new StructHeader(2, (short) 0x42));

        assertThat((Optional) result).containsSame(reader);

        verifyNoInteractions(reader);
    }

    @Test
    void getReaderShouldReturnEmptyOptionalWhenUnknownTagIsGiven() {
        var reader = mock(StructReader.class);
        var writer = mock(StructWriter.class);

        this.registry.registerReader((short) 0x41, reader);
        this.registry.registerWriter(Object.class, writer);

        var result = this.registry.getReader(new StructHeader(2, (short) 0x42));

        assertThat(result).isEmpty();

        verifyNoInteractions(reader);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void getWriterShouldReturnRegisteredWriter() {
        var writer = mock(StructWriter.class);
        this.registry.registerWriter(Object.class, writer);

        var result = this.registry.getWriter(new Object());

        assertThat((Optional) result).containsSame(writer);

        verifyNoInteractions(writer);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void getWriterShouldReturnRegisteredWriterForSubtype() {
        var writer = mock(StructWriter.class);
        this.registry.registerWriter(Object.class, writer);

        var result = this.registry.getWriter(42);

        assertThat((Optional) result).containsSame(writer);

        verifyNoInteractions(writer);
    }

    @Test
    void getWriterShouldReturnEmptyOptionalWhenUnknownTagIsGiven() {
        var reader = mock(StructReader.class);
        var writer = mock(StructWriter.class);

        this.registry.registerReader((short) 0x42, reader);
        this.registry.registerWriter(Integer.class, writer);

        var result = this.registry.getWriter(new Object());

        assertThat(result).isEmpty();
    }

    private static class StructRegistryImpl extends AbstractStructRegistry<Object, Object> {

        public StructRegistryImpl() {
            super(new HashMap<>(), new HashMap<>());
        }

        private void registerReader(short tag, StructReader<? super Object, ?> reader) {
            this.tagToReaderMap.put(tag, reader);
        }

        private void registerWriter(Class<?> type, StructWriter<? super Object, ? super Object> writer) {
            this.typeToWriterMap.put(type, writer);
        }
    }
}
