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
package org.neo4j.internal.id;

public class IdUtils {
    // [usss,ssss][siii,iiii][iiii,iiii][iiii,iiii] [iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii]
    private static final long MASK_USED = 0x80000000_00000000L;
    private static final int NUM_BITS_NUMBER_OF_IDS = 8;
    private static final int SHIFT_NUMBER_OF_IDS = Long.SIZE - 1 - NUM_BITS_NUMBER_OF_IDS;
    private static final long MASK_NUMBER_OF_IDS = ((1L << NUM_BITS_NUMBER_OF_IDS) - 1) << SHIFT_NUMBER_OF_IDS;
    private static final long MASK_ID = ~(MASK_USED | MASK_NUMBER_OF_IDS);

    static final long MAX_ID = MASK_ID;
    static final int MAX_NUMBER_OF_IDS = 1 << NUM_BITS_NUMBER_OF_IDS;

    /**
     * Combines an starting ID, the number of IDs available and whether or not the IDs are used into a single {@code long} for convenience.
     * This works with the assumption that IDs don't use the high 9 bits, something which is also verified in this call.
     *
     * @param id the ID, or for numberOfIds > 1 the starting ID.
     * @param numberOfIds number of IDs including the given ID this is about.
     * @param used whether or not the ID(s) are used.
     * @return ID, number of IDs and used combined into a single {@code long}.
     */
    public static long combinedIdAndNumberOfIds(long id, int numberOfIds, boolean used) {
        int storedNumberOfIds = numberOfIds - 1;
        if ((id & ~MASK_ID) != 0) {
            throw new IllegalArgumentException("ID " + id + " is too big");
        }
        if (storedNumberOfIds > 0xFF) {
            throw new IllegalArgumentException("Number of IDs " + numberOfIds + " is too big");
        }
        return id | (((long) storedNumberOfIds) << SHIFT_NUMBER_OF_IDS) | (used ? MASK_USED : 0);
    }

    public static long idFromCombinedId(long combinedId) {
        return combinedId & MASK_ID;
    }

    public static int numberOfIdsFromCombinedId(long combinedId) {
        return (int) ((combinedId & MASK_NUMBER_OF_IDS) >>> SHIFT_NUMBER_OF_IDS) + 1;
    }

    public static boolean usedFromCombinedId(long combined) {
        return (combined & MASK_USED) != 0;
    }
}
