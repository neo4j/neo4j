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
package org.neo4j.internal.kernel.api.security;

import org.neo4j.internal.helpers.NameUtil;

public class RelTypeSegment implements Segment
{
    private final String relType;

    public RelTypeSegment( String relType )
    {
        this.relType = relType;
    }

    public String getRelType()
    {
        return relType;
    }

    @Override
    public int hashCode()
    {
        return relType.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }

        if ( obj instanceof RelTypeSegment )
        {
            RelTypeSegment other = (RelTypeSegment) obj;
            if ( this.relType == null )
            {
                return other.relType == null;
            }
            else
            {
                return this.relType.equals( other.getRelType() );
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        return String.format( "RELATIONSHIP %s", relType == null ? "*" : relType );
    }

    @Override
    public String toCypherSnippet()
    {
        if ( relType == null )
        {
            return "RELATIONSHIP *";
        }
        else
        {
            return String.format( "RELATIONSHIP %s", NameUtil.escapeName( relType ) );
        }
    }

    @Override
    public boolean satisfies( Segment segment )
    {
        if ( segment instanceof RelTypeSegment )
        {
            var other = (RelTypeSegment) segment;
            return relType == null || relType.equals( other.relType );
        }
        return false;
    }

    public static final RelTypeSegment ALL = new RelTypeSegment( null );
}
