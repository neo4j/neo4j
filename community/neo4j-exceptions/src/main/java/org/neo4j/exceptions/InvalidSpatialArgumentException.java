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
package org.neo4j.exceptions;

import static java.lang.String.format;

import java.util.Arrays;

public class InvalidSpatialArgumentException extends InvalidArgumentException {

    public InvalidSpatialArgumentException(String message) {
        super(message);
    }

    public static InvalidSpatialArgumentException invalidDimension(String crs, int dimension, double... coordinate) {
        return new InvalidSpatialArgumentException(format(
                "Cannot create point, CRS %s expects %d dimensions, but got coordinates %s",
                crs, dimension, Arrays.toString(coordinate)));
    }

    public static InvalidSpatialArgumentException infiniteCoordinateValue(double... coordinate) {
        return new InvalidSpatialArgumentException(
                "Cannot create a point with non-finite coordinate values: " + Arrays.toString(coordinate));
    }

    public static InvalidSpatialArgumentException invalidGeographicCoordinates(double... coordinate) {
        return new InvalidSpatialArgumentException("Cannot create WGS84 point with invalid coordinate: "
                + Arrays.toString(coordinate) + ". Valid range for Y coordinate is [-90, 90].");
    }

    public static InvalidSpatialArgumentException invalidCoordinateSystem(int crs) {
        return invalidCoordinateSystem("code=" + crs);
    }

    public static InvalidSpatialArgumentException invalidCoordinateSystem(String crs) {
        return new InvalidSpatialArgumentException("Unknown coordinate reference system: " + crs);
    }
}
