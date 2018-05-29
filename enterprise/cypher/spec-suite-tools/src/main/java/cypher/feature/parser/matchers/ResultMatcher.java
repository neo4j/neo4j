/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package cypher.feature.parser.matchers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

import static cypher.feature.parser.matchers.UnorderedListMatcher.findMatch;

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
        List<RowMatcher> mutableCopy = new ArrayList<>( rowMatchers );
        final boolean[] matched = { true };
        List<String> columns = value.columns();

        value.accept( row ->
        {
            Map<String,Object> nextRow = new HashMap<>( columns.size() );
            for ( String col : columns )
            {
                nextRow.put( col, row.get( col ) );
            }

            int index = findMatch( mutableCopy, nextRow );
            if ( index < 0 )
            {
                matched[0] = false;
                return true;    // we always want to visit the next row
            }
            mutableCopy.remove( index );
            return true;
        } );

        boolean nothingLeftInMatcher = mutableCopy.isEmpty();
        return matched[0] && nothingLeftInMatcher;
    }

    public boolean matchesOrdered( Result value )
    {
        final int[] counter = { 0 };
        final boolean[] matched = { true };
        List<String> columns = value.columns();

        value.accept( row ->
        {
            Map<String,Object> nextRow = new HashMap<>( columns.size() );
            for ( String col : columns )
            {
                nextRow.put( col, row.get( col ) );
            }

            if ( counter[0] >= rowMatchers.size() )
            {
                matched[0] = false;
                return true;    // we always want to visit the next row
            }
            matched[0] &= rowMatchers.get( counter[0]++ ).matches( nextRow );

            return true;
        } );

        boolean nothingLeftInMatcher = counter[0] == rowMatchers.size();
        return matched[0] && nothingLeftInMatcher;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder( "Expected result of:\n" );
        int i = 1;
        for ( RowMatcher row : rowMatchers )
        {
            sb.append( "[" ).append( i++ ).append( "] " ).append( row ).append( "\n" );
        }
        return sb.toString();
    }
}
