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
package org.neo4j.packstream.struct;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractMutableStructRegistry<S> extends AbstractStructRegistry<S> {

    protected AbstractMutableStructRegistry() {
        this(new HashMap<>(), new HashMap<>());
    }

    protected AbstractMutableStructRegistry(
            Map<Short, StructReader<? extends S>> tagToReaderMap,
            Map<Class<?>, StructWriter<? super S>> typeToWriterMap) {
        super(tagToReaderMap, typeToWriterMap);
    }

    protected void registerReader(short tag, StructReader<? extends S> reader) {
        this.tagToReaderMap.put(tag, reader);
    }

    protected void registerReader(StructReader<? extends S> reader) {
        this.registerReader(reader.getTag(), reader);
    }

    @SuppressWarnings("unchecked")
    protected <T extends S> void registerWriter(Class<T> type, StructWriter<? super T> writer) {
        this.typeToWriterMap.put(type, (StructWriter<? super S>) writer);
    }
}
