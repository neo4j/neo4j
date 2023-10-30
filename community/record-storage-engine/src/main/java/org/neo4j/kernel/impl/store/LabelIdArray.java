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

import static java.lang.System.arraycopy;

import java.util.Arrays;

public class LabelIdArray {
    private LabelIdArray() {}

    static int[] concatAndSort(int[] existing, int additional) {
        assertNotContains(existing, additional);

        int[] result = new int[existing.length + 1];
        arraycopy(existing, 0, result, 0, existing.length);
        result[existing.length] = additional;
        Arrays.sort(result);
        return result;
    }

    private static void assertNotContains(int[] existingLabels, int labelId) {
        if (Arrays.binarySearch(existingLabels, labelId) >= 0) {
            throw new IllegalStateException("Label " + labelId + " already exists.");
        }
    }

    static int[] filter(int[] ids, int excludeId) {
        boolean found = false;
        for (long id : ids) {
            if (id == excludeId) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw new IllegalStateException("Label " + excludeId + " not found.");
        }

        int[] result = new int[ids.length - 1];
        int writerIndex = 0;
        for (int id : ids) {
            if (id != excludeId) {
                result[writerIndex++] = id;
            }
        }
        return result;
    }

    public static long[] prependNodeId(long nodeId, int[] labelIds) {
        long[] result = new long[labelIds.length + 1];
        result[0] = nodeId;
        for (int i = 0; i < labelIds.length; i++) {
            result[i + 1] = labelIds[i];
        }
        return result;
    }

    public static int[] stripNodeId(long[] storedLongs) {
        long[] recordLabels = Arrays.copyOfRange(storedLongs, 1, storedLongs.length);
        int[] labelIds = new int[recordLabels.length];
        for (int i = 0; i < labelIds.length; i++) {
            labelIds[i] = (int) recordLabels[i];
        }
        return labelIds;
    }
}
