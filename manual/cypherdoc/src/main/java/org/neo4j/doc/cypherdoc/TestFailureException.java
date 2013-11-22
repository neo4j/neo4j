/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.doc.cypherdoc;

import java.util.List;

class TestFailureException extends RuntimeException
{
    final Result result;

    TestFailureException( Result result, List<String> failedTests )
    {
        super( message( failedTests ) );
        this.result = result;
    }

    private static String message( List<String> failedTests )
    {
        StringBuilder message = new StringBuilder( "Query validation failed:" );
        for ( String test : failedTests )
        {
            message.append( CypherDoc.EOL )
                   .append( "\tQuery result doesn't contain the string '" ).append( test ).append( "'." );
        }
        return message.toString();
    }

    @Override
    public String toString()
    {
        StringBuilder message = new StringBuilder( getMessage() );
        message.append( CypherDoc.EOL )
               .append( "Query:" ).append( CypherDoc.EOL ).append( '\t' )
               .append( CypherDoc.indent( result.query ) );
        message.append( CypherDoc.EOL )
               .append( "Result:" ).append( CypherDoc.EOL ).append( '\t' )
               .append( CypherDoc.indent( result.text ) );
        message.append( CypherDoc.EOL )
               .append( "Profile:" ).append( CypherDoc.EOL ).append( '\t' )
               .append( CypherDoc.indent( result.profile ) );
        return message.toString();
    }
}
