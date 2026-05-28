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
package org.neo4j.bolt.negotiation.version;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

public final class ProtocolVersionReducer {
    private short currentMajor = -1;
    private short initialMinor = -1;
    private short currentMinor = -1;

    private final List<ProtocolVersion> versions = new ArrayList<>();

    public static List<ProtocolVersion> canonicalize(Iterable<ProtocolVersion> versions) {
        var reducer = new ProtocolVersionReducer();

        StreamSupport.stream(versions.spliterator(), false)
                .flatMap(version -> version.unwind().stream())
                .sorted(ProtocolVersion::compareTo)
                .forEachOrdered(reducer::submit);

        return reducer.finalizeResult();
    }

    public void submit(ProtocolVersion version) {
        if (version.major() != this.currentMajor) {
            // when a new major version comes along, the range has to be cut off and a new one started
            this.rollMajor(version);
        } else if (this.currentMinor + 1 != version.minor()) {
            // when there is a disconnect within the version sequence (e.g., there is an unsupported
            // version within the chain), the range has to be cut off and a new one started
            this.rollMajor(version);
        } else {
            // if the minor is sequential to its predecessor, we continue as is - if this is the last
            // version within the chain, it will be finalized via finalizeResult()
            this.currentMinor = version.minor();
        }
    }

    private void rollMajor(ProtocolVersion version) {
        var previousMajor = this.currentMajor;
        var initialMinor = this.initialMinor;
        var previousMinor = this.currentMinor;

        this.currentMajor = version.major();
        this.initialMinor = this.currentMinor = version.minor();

        if (previousMajor == -1) {
            return;
        }

        this.onFinalizeMajor(previousMajor, initialMinor, previousMinor);
    }

    private void onFinalizeMajor(short major, short initialMinor, short latestMinor) {
        var range = latestMinor - initialMinor;
        this.versions.add(new ProtocolVersion(major, latestMinor, range));
    }

    public List<ProtocolVersion> finalizeResult() {
        this.rollMajor(ProtocolVersion.INVALID);

        return new ArrayList<>(this.versions);
    }

    public void reset() {
        this.currentMajor = -1;
        this.initialMinor = -1;
        this.currentMinor = -1;
    }
}
