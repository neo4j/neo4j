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

public class UserSegment implements Segment
{
    private final String username;

    public UserSegment( String username )
    {
        this.username = username;
    }

    @Override
    public boolean satisfies( Segment segment )
    {
        if ( segment instanceof UserSegment )
        {
            var other = (UserSegment) segment;
            return username == null || username.equals( other.username );
        }
        return false;
    }

    public String toCypherSnippet()
    {
        if ( username == null )
        {
            return "*";
        }
        else
        {
            return NameUtil.escapeName( username );
        }
    }

    @Override
    public String toString()
    {
        return username == null ? "*" : username;
    }

    public static final UserSegment ALL = new UserSegment( null );
}
