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
package org.neo4j.dbms.database;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.neo4j.graphdb.Transaction;

public class KnownSystemComponentVersions<T extends KnownSystemComponentVersion> {
    public final T noComponentVersion;
    private final ArrayList<T> knownComponentVersions = new ArrayList<>();

    public KnownSystemComponentVersions(T noComponentVersion) {
        this.noComponentVersion = noComponentVersion;
    }

    public void add(T version) {
        knownComponentVersions.add(version);
    }

    public T detectCurrentComponentVersion(Transaction tx) {
        List<T> sortedVersions = new ArrayList<>(knownComponentVersions);
        sortedVersions.sort(Comparator.comparingInt(v -> v.version));
        Collections.reverse(sortedVersions); // Sort from most recent to oldest
        for (T version : sortedVersions) {
            if (version.detected(tx)) {
                return version;
            }
        }
        return noComponentVersion;
    }

    public T latestComponentVersion() {
        T latest = noComponentVersion;
        for (T knownComponentVersion : knownComponentVersions) {
            if (knownComponentVersion.version > latest.version) {
                latest = knownComponentVersion;
            }
        }
        return latest;
    }

    public T findComponentVersion(ComponentVersion componentVersion) {
        for (T knownComponentVersion : knownComponentVersions) {
            if (knownComponentVersion.version == componentVersion.getVersion()) {
                return knownComponentVersion;
            }
        }
        return noComponentVersion;
    }
}
