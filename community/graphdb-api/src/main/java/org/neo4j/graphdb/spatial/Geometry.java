/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.graphdb.spatial;

import java.util.List;


/**
 * A geometry is defined by a list of coordinates and a coordinate reference system.
 */
public interface Geometry
{
    /**
     * Get string description of most specific type of this instance
     *
     * @return The instance type implementing Geometry
     */
    String getGeometryType();

    /**
     * Get all coordinates of the geometry.
     *
     * @return The coordinates of the geometry.
     */
    List<Coordinate> getCoordinates();

    /**
     * Returns the coordinate reference system associated with the geometry
     *
     * @return A ${@link CRS} associated with the geometry
     */
    CRS getCRS();
}
