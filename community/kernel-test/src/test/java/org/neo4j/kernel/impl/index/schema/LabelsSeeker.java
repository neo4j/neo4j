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
package org.neo4j.kernel.impl.index.schema;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.helpers.collection.Pair;

final class LabelsSeeker implements Seeker<TokenScanKey, TokenScanValue> {
    private boolean closed;
    private final List<Pair<TokenScanKey, TokenScanValue>> entries;
    private final boolean ascending;
    private final TokenScanLayout layout = new TokenScanLayout();
    private final int stride;
    private int cursor;

    LabelsSeeker(List<Pair<TokenScanKey, TokenScanValue>> entries, boolean ascending) {
        this.entries = entries;
        this.ascending = ascending;
        this.stride = ascending ? 1 : -1;
        this.cursor = cursorStartValue();
    }

    private int cursorStartValue() {
        return ascending ? -1 : entries.size();
    }

    @Override
    public TokenScanKey key() {
        Assertions.assertFalse(closed);
        return entries.get(cursor).first();
    }

    @Override
    public TokenScanValue value() {
        Assertions.assertFalse(closed);
        return entries.get(cursor).other();
    }

    @Override
    public boolean next() {
        int candidate = cursor + stride;
        if (candidate < 0 || candidate >= entries.size()) {
            close();
            return false;
        }
        cursor += stride;
        return true;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void reinitializeToNewRange(TokenScanKey fromInclusive, TokenScanKey toExclusive) {
        // we can ignore toExclusive since it is used to determine index order,
        // but in this impl it is already known
        while (cursor == cursorStartValue() || isBefore(fromInclusive)) {
            if (!next()) {
                return;
            }
        }
        cursor -= stride;
    }

    private boolean isBefore(TokenScanKey target) {
        int comparison = layout.compare(entries.get(cursor).first(), target);
        return ascending ? comparison < 0 : comparison > 0;
    }
}
