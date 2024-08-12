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
package org.neo4j.packstream.error.reader;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.packstream.io.TypeMarker;

public class UnexpectedTypeMarkerException extends UnexpectedTypeException {
    private final TypeMarker expectedMarker;
    private final TypeMarker actualMarker;

    public UnexpectedTypeMarkerException(TypeMarker expected, TypeMarker actual) {
        super("Expected " + expected + " but got " + actual, expected.getType(), actual.getType());

        this.expectedMarker = expected;
        this.actualMarker = actual;
    }

    public UnexpectedTypeMarkerException(ErrorGqlStatusObject gqlStatusObject, TypeMarker expected, TypeMarker actual) {
        super(gqlStatusObject, "Expected " + expected + " but got " + actual, expected.getType(), actual.getType());

        this.expectedMarker = expected;
        this.actualMarker = actual;
    }

    public TypeMarker getExpectedMarker() {
        return this.expectedMarker;
    }

    public TypeMarker getActualMarker() {
        return this.actualMarker;
    }
}
