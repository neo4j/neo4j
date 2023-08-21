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
package org.neo4j.server.http.cypher.format.jolt;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

import java.util.function.Function;
import org.neo4j.graphdb.spatial.Point;

final class PointToWKT implements Function<Point, String> {
    @Override
    public String apply(Point point) {
        var coordinates = point.getCoordinate().getCoordinate();
        return "SRID=" + point.getCRS().getCode()
                + ";POINT"
                + (coordinates.length == 3 ? " Z " : "")
                + "("
                + stream(coordinates).mapToObj(String::valueOf).collect(joining(" "))
                + ")";
    }
}
