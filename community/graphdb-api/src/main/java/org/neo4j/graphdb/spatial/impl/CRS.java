/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.graphdb.spatial.impl;

/**
 * Definition of {@link org.neo4j.graphdb.spatial.CRS} that are supported by Neo4j.
 *
 */
public enum CRS implements org.neo4j.graphdb.spatial.CRS
{
    Cartesian( 7203, "cartesian", "http://spatialreference.org/ref/sr-org/7203/" ),
    Cartesian_3D( 9157, "cartesian-3d", "http://spatialreference.org/ref/sr-org/9157/" ),
    WGS84( 4326, "wgs-84", "http://spatialreference.org/ref/epsg/4326/" ),
    WGS84_3D( 4979, "wgs-84-3d", "http://spatialreference.org/ref/epsg/4979/" );

    private final int code;
    private final String type;
    private final String href;

    CRS( int code, String type, String href )
    {

        this.code = code;
        this.type = type;
        this.href = href;
    }

    @Override
    public int getCode()
    {
        return code;
    }

    @Override
    public String getType()
    {
        return type;
    }

    @Override
    public String getHref()
    {
        return href;
    }
}
