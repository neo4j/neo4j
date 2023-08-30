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
package org.neo4j.index.internal.gbptree;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * Holds VALUE and `defined` flag. See {@link #valueAt(PageCursor, ValueHolder, int, CursorContext)}
 * and {@link #keyValueAt(PageCursor, Object, ValueHolder, int, CursorContext)}
 *
 * @param <VALUE>
 */
public final class ValueHolder<VALUE> {
    VALUE value;
    boolean defined;

    public ValueHolder(VALUE value) {
        this(value, false);
    }

    public ValueHolder(VALUE value, boolean defined) {
        this.value = value;
        this.defined = defined;
    }
}
