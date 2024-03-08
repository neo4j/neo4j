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
package org.neo4j.kernel.impl.store.format.standard;

import org.neo4j.storageengine.api.StoreFormatLimits;

/**
 * Common low limit format settings.
 */
public final class StandardFormatSettings {
    public static final int PROPERTY_TOKEN_MAXIMUM_ID_BITS = 24;
    static final int NODE_MAXIMUM_ID_BITS = 35;
    static final int RELATIONSHIP_MAXIMUM_ID_BITS = 35;
    static final int PROPERTY_MAXIMUM_ID_BITS = 36;
    public static final int DYNAMIC_MAXIMUM_ID_BITS = 36;
    // 31 bits is too much in some VMs. The token registry has an HashMap that internally uses
    // an int[] for its keys. That array size is limited to Integer.MAX_VALUE - header.
    // Not limiting ids to less than 31 bits here since depending on VM someone might have managed to
    // create a token bigger than the Integer.MAX_VALUE - 8 we would want to limit it to.
    public static final int LABEL_TOKEN_MAXIMUM_ID_BITS = 31;
    public static final int RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS = 16;
    static final int RELATIONSHIP_GROUP_MAXIMUM_ID_BITS = 35;
    public static final int SCHEMA_RECORD_ID_BITS = 32; // Should ideally be less than PROPERTY_MAXIMUM_ID_BITS.

    public static final StoreFormatLimits LIMITS = new StoreFormatLimits(
            bitsToMaxId(LABEL_TOKEN_MAXIMUM_ID_BITS),
            bitsToMaxId(RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS),
            bitsToMaxId(PROPERTY_TOKEN_MAXIMUM_ID_BITS),
            bitsToMaxId(NODE_MAXIMUM_ID_BITS),
            bitsToMaxId(RELATIONSHIP_MAXIMUM_ID_BITS));

    public static long bitsToMaxId(int idBits) {
        return (1L << idBits) - 1;
    }

    private StandardFormatSettings() {}
}
