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

import static org.neo4j.storageengine.api.StoreVersionUserStringProvider.formatVersion;

import java.util.Objects;

public class StoreVersionIdentifier implements StoreVersionUserStringProvider {

    private final String storageEngineName;
    private final String formatName;
    private final int majorVersion;
    private final int minorVersion;

    public StoreVersionIdentifier(String storageEngineName, String formatName, int majorVersion, int minorVersion) {
        this.storageEngineName = storageEngineName;
        this.formatName = formatName;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    public String getStorageEngineName() {
        return storageEngineName;
    }

    public String getFormatName() {
        return formatName;
    }

    public int getMajorVersion() {
        return majorVersion;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    boolean isSameOrUpgradeSuccessor(StoreVersionIdentifier anotherVersionIdentifier) {
        return majorVersion == anotherVersionIdentifier.majorVersion
                && minorVersion <= anotherVersionIdentifier.minorVersion
                && storageEngineName.equals(anotherVersionIdentifier.storageEngineName)
                && formatName.equals(anotherVersionIdentifier.formatName);
    }

    /**
     * End-user friendly representation of the store version part of the ID.
     * <p>
     * The result of this method should be used in logging, error messages and similar cases,
     * when the store version needs to be represented to the end user.
     */
    @Override
    public String getStoreVersionUserString() {
        return formatVersion(storageEngineName, formatName, majorVersion, minorVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoreVersionIdentifier that = (StoreVersionIdentifier) o;
        return majorVersion == that.majorVersion
                && minorVersion == that.minorVersion
                && storageEngineName.equals(that.storageEngineName)
                && formatName.equals(that.formatName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageEngineName, formatName, majorVersion, minorVersion);
    }

    @Override
    public String toString() {
        return getStoreVersionUserString();
    }
}
