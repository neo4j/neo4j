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

import static org.neo4j.index.internal.gbptree.Layout.namedIdentifier;

import org.neo4j.io.pagecache.PageCursor;

public class LongKeyLayout extends KeyLayout.Adapter<LongKeyLayout.LongKey> {
    protected LongKeyLayout() {
        super(true, namedIdentifier("long", 1234), 1, 0);
    }

    @Override
    public LongKey newKey() {
        return new LongKey();
    }

    public LongKey newKey(long id) {
        return new LongKey().initialize(id);
    }

    @Override
    public LongKey copyKey(LongKey key, LongKey into) {
        into.id = key.id;
        return into;
    }

    @Override
    public int keySize(LongKey key) {
        return Long.BYTES;
    }

    @Override
    public void writeKey(PageCursor cursor, LongKey key) {
        cursor.putLong(key.id);
    }

    @Override
    public void readKey(PageCursor cursor, LongKey into, int keySize) {
        into.id = cursor.getLong();
    }

    @Override
    public void initializeAsLowest(LongKey key) {
        key.id = Long.MIN_VALUE;
    }

    @Override
    public void initializeAsHighest(LongKey key) {
        key.id = Long.MAX_VALUE;
    }

    @Override
    public int compare(LongKey o1, LongKey o2) {
        return Long.compare(o1.id, o2.id);
    }

    static class LongKey {
        long id;

        LongKey initialize(long id) {
            this.id = id;
            return this;
        }

        // Implements hashCode to play nice with the root mappings cache. hashCode/equals not quite needed otherwise

        @Override
        public int hashCode() {
            return Long.hashCode(id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LongKey longKey = (LongKey) o;
            return id == longKey.id;
        }

        @Override
        public String toString() {
            return "LongKey{" + "id=" + id + '}';
        }
    }
}
