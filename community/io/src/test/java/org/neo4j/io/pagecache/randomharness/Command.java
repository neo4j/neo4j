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
package org.neo4j.io.pagecache.randomharness;

/**
 * An enum of the commands that the RandomPageCacheTestHarness can perform, and their default probability factors.
 */
public enum Command
{
    ReadRecord( 0.3 ),
    WriteRecord( 0.6 ),
    FlushFile( 0.06 ),
    FlushCache( 0.02 ),
    MapFile( 0.01 ),
    UnmapFile( 0.01 );

    private final double defaultProbability;

    Command( double defaultProbability )
    {
        this.defaultProbability = defaultProbability;
    }

    public double getDefaultProbabilityFactor()
    {
        return defaultProbability;
    }
}
