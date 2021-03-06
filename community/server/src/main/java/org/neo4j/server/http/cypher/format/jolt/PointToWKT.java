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
package org.neo4j.server.http.cypher.format.jolt;

import java.util.function.Function;

import org.neo4j.graphdb.spatial.Point;

import static java.util.stream.Collectors.joining;

final class PointToWKT implements Function<Point,String>
{
    @Override
    public String apply( Point point )
    {
        var coordinates = point.getCoordinate().getCoordinate();
        var wkt = new StringBuilder()
                .append( "SRID=" )
                .append( point.getCRS().getCode() )
                .append( ";POINT" )
                .append( coordinates.size() == 3 ? " Z " : "" )
                .append( "(" )
                .append( coordinates.stream().map( String::valueOf ).collect( joining( " " ) ) )
                .append( ")" );
        return wkt.toString();
    }
}
