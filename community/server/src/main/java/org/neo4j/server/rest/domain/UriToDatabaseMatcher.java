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
package org.neo4j.server.rest.domain;

import java.net.URI;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// FIXME: remove this class? only used from test case
public class UriToDatabaseMatcher
{

    private static ArrayList<Tuple> tuples = new ArrayList<Tuple>();

    public GraphDatabaseName lookup( URI requestUri )
    {

        for ( int i = 0; i < tuples.size(); i++ )
        {
            Tuple t = tuples.get( i );

            // Matchers aren't thread-safe, so they have to be created
            // per-request
            Matcher matcher = t.pattern.matcher( requestUri.getPath() );

            if ( matcher.matches() )
            {
                return t.graphDatabaseName;
            }
        }

        return GraphDatabaseName.NO_NAME;
    }

    /**
     * Adds a mapping the given database using the default URI
     * (/{database_name})
     * 
     * @param databaseName
     */
    public void add( GraphDatabaseName databaseName )
    {
        // Patterns are thread safe and immutable, so compile them once only
        tuples.add( new Tuple( Pattern.compile( "^.*" + databaseName.getName() + ".*$" ), databaseName ) );
    }

    @Override
    public String toString()
    {

        StringBuilder sb = new StringBuilder();
        for ( int i = tuples.size() - 1; i >= 0; i-- )
        {
            Tuple t = tuples.get( i );

            sb.append( t );

        }

        return sb.toString();

    }

    private static class Tuple
    {
        public GraphDatabaseName graphDatabaseName;
        public Pattern pattern;

        public Tuple( Pattern pattern, GraphDatabaseName graphDatabaseName )
        {
            this.pattern = pattern;
            this.graphDatabaseName = graphDatabaseName;

        }

        @Override
        public String toString()
        {
            return pattern.toString() + " => " + graphDatabaseName + System.lineSeparator();
        }
    }
}
