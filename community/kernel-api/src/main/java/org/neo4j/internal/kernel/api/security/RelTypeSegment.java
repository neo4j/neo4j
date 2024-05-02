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
package org.neo4j.internal.kernel.api.security;

import org.neo4j.internal.helpers.NameUtil;

public record RelTypeSegment(String relType) implements Segment {

    @Override
    public boolean satisfies(Segment segment) {
        if (segment instanceof RelTypeSegment other) {
            return relType == null || relType.equals(other.relType);
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("RELATIONSHIP(%s)", nullToStar(relType));
    }

    @Override
    public String toCypherSnippet() {
        if (relType == null) {
            return "RELATIONSHIP *";
        } else {
            return String.format("RELATIONSHIP %s", NameUtil.escapeName(relType));
        }
    }

    public static final RelTypeSegment ALL = new RelTypeSegment(null);
}
