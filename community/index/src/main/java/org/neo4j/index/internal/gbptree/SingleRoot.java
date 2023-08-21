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

public final class SingleRoot implements KeyLayout<SingleRoot> {
    public static final SingleRoot SINGLE_ROOT = new SingleRoot();

    private SingleRoot() {}

    @Override
    public boolean fixedSize() {
        return true;
    }

    @Override
    public SingleRoot newKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SingleRoot copyKey(SingleRoot singleRoot, SingleRoot into) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int keySize(SingleRoot singleRoot) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeKey(PageCursor cursor, SingleRoot singleRoot) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readKey(PageCursor cursor, SingleRoot into, int keySize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long identifier() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int majorVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int minorVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean compatibleWith(long layoutIdentifier, int majorVersion, int minorVersion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void initializeAsLowest(SingleRoot singleRoot) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void initializeAsHighest(SingleRoot singleRoot) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compare(SingleRoot o1, SingleRoot o2) {
        throw new UnsupportedOperationException();
    }
}
