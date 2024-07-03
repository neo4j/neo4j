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

public class ProcedureSegment implements Segment
{
    private final String procedure;

    public ProcedureSegment( String procedure )
    {
        this.procedure = procedure;
    }

    public String getProcedure()
    {
        return procedure;
    }

    @Override
    public boolean satisfies( Segment segment )
    {
        if ( segment instanceof ProcedureSegment )
        {
            var other = (ProcedureSegment) segment;
            return procedure == null || procedure.equals( other.procedure );
        }
        return false;
    }

    public String toCypherSnippet()
    {
        if ( procedure == null )
        {
            return "*";
        }
        else
        {
            return NameUtil.escapeGlob( procedure );
        }
    }

    @Override
    public String toString()
    {
        return procedure == null ? "*" : procedure;
    }

    public static final ProcedureSegment ALL = new ProcedureSegment( null );
}
