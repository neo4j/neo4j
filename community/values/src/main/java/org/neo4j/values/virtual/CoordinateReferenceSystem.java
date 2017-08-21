/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.values.virtual;

public enum CoordinateReferenceSystem
{
    Cartesian( "cartesian", 7203, "http://spatialreference.org/ref/sr-org/7203/" ),
    WGS84( "WGS-84", 4326, "http://spatialreference.org/ref/epsg/4326/" );

    public final String name;
    public final int code;
    public final String href;

    CoordinateReferenceSystem( String name, int code, String href )
    {
        this.name = name;
        this.code = code;
        this.href = href;
    }

    public int code()
    {
        return code;
    }

    public String type()
    {
        return name;
    }

    public String href()
    {
        return href;
    }
}
