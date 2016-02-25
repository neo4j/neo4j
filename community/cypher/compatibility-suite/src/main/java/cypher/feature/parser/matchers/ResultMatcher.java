/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package cypher.feature.parser.matchers;

import java.util.List;

import org.neo4j.graphdb.Result;

public class ResultMatcher implements Matcher<Result>
{
    private final List<RowMatcher> rowMatchers;

    public ResultMatcher( List<RowMatcher> rowMatchers )
    {
        this.rowMatchers = rowMatchers;
    }

    @Override
    public boolean matches( Result value )
    {
        boolean matches = true;
        int counter = 0;
        while ( value.hasNext() && counter < rowMatchers.size() )
        {
            matches &= rowMatchers.get( counter++ ).matches( value.next() );
        }

        boolean nothingLeftInReal = !value.hasNext();
        boolean nothingLeftInMatcher = counter == rowMatchers.size();
        return matches && nothingLeftInMatcher && nothingLeftInReal;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder( "ResultMatcher of:\n" );
        for ( RowMatcher row : rowMatchers )
        {
            sb.append( row ).append( "\n" );
        }
        return sb.toString();
    }
}
