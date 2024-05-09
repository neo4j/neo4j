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

public class LabelSegment implements Segment
{
    private final String label;

    public LabelSegment( String label )
    {
        this.label = label;
    }

    public String getLabel()
    {
        return label;
    }

    @Override
    public int hashCode()
    {
        return label.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }

        if ( obj instanceof LabelSegment )
        {
            LabelSegment other = (LabelSegment) obj;
            if ( this.label == null )
            {
                return other.label == null;
            }
            else
            {
                return this.label.equals( other.getLabel() );
            }
        }
        return false;
    }

    @Override
    public String toCypherSnippet()
    {
        if ( label == null )
        {
            return "NODE *";
        }
        else
        {
            return String.format( "NODE %s", NameUtil.escapeName( label ) );
        }
    }

    @Override
    public String toString()
    {
        return String.format( "NODE %s", label == null ? "*" : label );
    }

    @Override
    public boolean satisfies( Segment segment )
    {
        if ( segment instanceof LabelSegment )
        {
            var other = (LabelSegment) segment;
            return label == null || label.equals( other.label );
        }
        return false;
    }

    public static final LabelSegment ALL = new LabelSegment( null );
}
