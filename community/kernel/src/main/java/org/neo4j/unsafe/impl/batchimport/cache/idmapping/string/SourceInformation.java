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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import java.util.List;

import org.neo4j.csv.reader.SourceTraceability;

/**
 * Encodes source id (effectively and id refering to a file name or similar,
 * i.e {@link SourceTraceability#sourceDescription()}, group id and line number.
 */
class SourceInformation implements Cloneable
{
    static final long LINE_NUMBER_MASK = 0xFFFF_FFFFFFFFL;

    int sourceId;
    long lineNumber;

    SourceInformation decode( long sourceInformation )
    {
        sourceId = (int) ((sourceInformation & ~LINE_NUMBER_MASK) >>> 48); // >>> we don't want the sign to matter
        lineNumber = (sourceInformation & LINE_NUMBER_MASK);
        return this;
    }

    static long encodeSourceInformation( int sourceId, long lineNumber )
    {
        if ( (sourceId & 0xFFFF0000) != 0 )
        {
            throw new IllegalArgumentException( "Collisions in too many sources (currently at " + sourceId + ")" );
        }
        if ( (lineNumber & ~LINE_NUMBER_MASK) != 0 )
        {
            throw new IllegalArgumentException( "Collision in source with too many lines (" + lineNumber + ")" );
        }

        return ((long)sourceId << 48) | lineNumber;
    }

    public String describe( List<String> sourceDescriptions )
    {
        return sourceDescriptions.get( sourceId ) + ":" + lineNumber;
    }
}
