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
package org.neo4j.storageengine.api;

/**
 * A common StoreIdentifier for both log files that use StoreId, and those that only use a single long value
 * to match against store files StoreId.
 */
public record StoreIdentifier(long randomValueIdentifier, StoreId storeId) {
    public static final StoreIdentifier UNKNOWN = new StoreIdentifier(StoreId.UNKNOWN.getRandom(), StoreId.UNKNOWN);

    public static StoreIdentifier newStoreIdentifier(long random) {
        return new StoreIdentifier(random, null);
    }

    public static StoreIdentifier newStoreIdentifier(StoreId storeId) {
        return new StoreIdentifier(storeId.getRandom(), storeId);
    }

    public boolean isSameOrUpgradeSuccessor(StoreId anotherId) {
        if (storeId != null) {
            return storeId.isSameOrUpgradeSuccessor(anotherId);
        }
        return randomValueIdentifier == anotherId.getRandom();
    }

    public boolean matches(StoreId anotherId) {
        if (anotherId == null) {
            return false;
        }
        if (storeId != null) {
            return storeId.equals(anotherId);
        }
        return randomValueIdentifier == anotherId.getRandom();
    }

    public boolean matches(StoreIdentifier anotherIdentifier) {
        if (anotherIdentifier == null) {
            return false;
        }
        return randomValueIdentifier == anotherIdentifier.randomValueIdentifier;
    }

    public boolean isUnknown() {
        return randomValueIdentifier == UNKNOWN.randomValueIdentifier;
    }

    @Override
    public String toString() {
        return "StoreIdentifier=" + (storeId != null ? storeId : randomValueIdentifier);
    }
}
