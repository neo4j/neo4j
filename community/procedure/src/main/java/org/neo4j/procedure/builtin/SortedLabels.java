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
package org.neo4j.procedure.builtin;

import java.util.Arrays;
import org.neo4j.internal.kernel.api.TokenSet;

public class SortedLabels {
    private final int[] labels;

    private SortedLabels(int[] labels) {
        this.labels = labels;
    }

    public static SortedLabels from(int[] labels) {
        Arrays.sort(labels);
        return new SortedLabels(labels);
    }

    static SortedLabels from(TokenSet tokenSet) {
        return from(tokenSet.all());
    }

    private int[] all() {
        return labels;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(labels);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SortedLabels) {
            int[] input = ((SortedLabels) obj).all();
            return Arrays.equals(labels, input);
        }
        return false;
    }

    public int numberOfLabels() {
        return labels.length;
    }

    public Integer label(int offset) {
        return labels[offset];
    }
}
