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

@FunctionalInterface
public interface StoreVersionUserStringProvider {
    String BETA_SUFFIX = "b";

    /**
     * Provides store version in the form presented to the end user.
     * <p>
     * The result of this method should be used in logging, error messages and similar cases,
     * when the store version needs to be represented to the end user.
     * <p>
     * Don't use this as version identifier in code and don't try to parse it or interpret the string in any other way!
     * When you are tempted to use it in that way, use {@link StoreId} or {@link StoreVersion} instead!
     */
    String getStoreVersionUserString();

    static String formatVersion(String storageEngineName, String formatFamilyName, int majorVersion, int minorVersion) {
        String majorVersionAddition = "";

        // Allow beta versions
        if (majorVersion < 0) {
            majorVersion = -majorVersion;
            majorVersionAddition = BETA_SUFFIX;
        }

        return storageEngineName + "-" + formatFamilyName + "-" + majorVersion + majorVersionAddition + "."
                + minorVersion;
    }
}
