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
package org.neo4j.kernel.impl.newapi;

import java.util.Arrays;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.internal.kernel.api.TokenSet;

public class Labels implements TokenSet {
    private final int[] labelIds;

    private Labels(int[] labelIds) {
        this.labelIds = labelIds;
    }

    public static Labels from(int... labels) {
        return new Labels(labels);
    }

    static Labels from(IntSet set) {
        return new Labels(set.toArray());
    }

    static Labels from(LongSet set) {
        final int[] labelIds = new int[set.size()];
        MutableInt index = new MutableInt();
        set.each(element -> labelIds[index.getAndIncrement()] = (int) element);
        return new Labels(labelIds);
    }

    @Override
    public int numberOfTokens() {
        return labelIds.length;
    }

    @Override
    public int token(int offset) {
        return labelIds[offset];
    }

    @Override
    public boolean contains(int token) {
        // It may look tempting to use binary search
        // however doing a linear search is actually faster for reasonable
        // label sizes (≤100 labels)
        for (int label : labelIds) {
            if (label == token) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "Labels" + Arrays.toString(labelIds);
    }

    @Override
    public int[] all() {
        return labelIds;
    }
}
