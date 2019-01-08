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


/**
 * A coordinate reference system (CRS) determines how a ${@link Coordinate} should be interpreted
 * <p>
 * The CRS is defined by three properties a code, a type, and a link to CRS parameters on the web.
 * Example:
 * <code>
 * {
 * code: 4326,
 * type: "WGS-84",
 * href: "http://spatialreference.org/ref/epsg/4326/"
 * }
 * </code>
 */
public interface CRS
{

    /**
     * The numerical code associated with the CRS
     *
     * @return a numerical code associated with the CRS
     */
    int getCode();

    /**
     * The type of the CRS is a descriptive name, indicating which CRS is used
     *
     * @return the type of the CRS
     */
    String getType();

    /**
     * A link uniquely identifying the CRS.
     *
     * @return A link to where the CRS is described.
     */
    String getHref();
}
