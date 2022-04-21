/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api;

import java.security.SecureRandom;
import java.util.Objects;
import java.util.Random;

public final class LegacyStoreId {
    public static final LegacyStoreId UNKNOWN = new LegacyStoreId(-1, -1, -1);

    private static final Random r = new SecureRandom();

    private final long creationTime;
    private final long randomId;
    private final long storeVersion;

    public LegacyStoreId(long storeVersion) {
        long currentTimeMillis = System.currentTimeMillis();
        long randomLong = r.nextLong();
        this.storeVersion = storeVersion;
        this.creationTime = currentTimeMillis;
        this.randomId = randomLong;
    }

    public LegacyStoreId(long creationTime, long randomId, long storeVersion) {
        this.creationTime = creationTime;
        this.randomId = randomId;
        this.storeVersion = storeVersion;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getRandomId() {
        return randomId;
    }

    public long getStoreVersion() {
        return storeVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LegacyStoreId storeId = (LegacyStoreId) o;
        return creationTime == storeId.creationTime
                && randomId == storeId.randomId
                && storeVersion == storeId.storeVersion;
    }

    public boolean equalsIgnoringVersion(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LegacyStoreId storeId = (LegacyStoreId) o;
        return creationTime == storeId.creationTime && randomId == storeId.randomId;
    }

    public boolean compatibleIncludingMinorUpgrade(
            StorageEngineFactory storageEngineFactory, LegacyStoreId otherStoreId) {
        if (!equalsIgnoringVersion(otherStoreId)) {
            return false; // Different store, not compatible
        }
        if (getStoreVersion() == otherStoreId.getStoreVersion()) {
            return true; // Same store, same version, compatible
        }

        return storageEngineFactory
                .rollingUpgradeCompatibility()
                .isVersionCompatibleForRollingUpgrade(getStoreVersion(), otherStoreId.getStoreVersion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(creationTime, randomId, storeVersion);
    }

    @Override
    public String toString() {
        return "StoreId{" + "creationTime="
                + creationTime + ", randomId="
                + randomId + ", storeVersion="
                + storeVersion + '}';
    }
}
