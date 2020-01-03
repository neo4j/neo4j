/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.query;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.values.storable.Values.stringValue;

public class QueryObfuscation
{
    private static final Pattern PASSWORD_PATTERN = Pattern.compile(
            // call signature
            "(?:(?i)call)\\s+(?:dbms(?:\\.security)?\\.change(?:User)?Password|dbms\\.security\\.createUser)\\(\\s*" +
            // optional username parameter, in single, double quotes, or parametrized
            "(?:\\s*(?:'(?:(?<=\\\\)'|[^'])*'|\"(?:(?<=\\\\)\"|[^\"])*\"|[^,]*)\\s*,)?" +
            // password parameter, in single, double quotes, or parametrized
            "\\s*('(?:(?<=\\\\)'|[^'])*'|\"(?:(?<=\\\\)\"|[^\"])*\"|\\$\\w*|\\{\\w*})" );

    static final TextValue OBFUSCATED = stringValue( "******" );
    static final String OBFUSCATED_LITERAL = "'******'";

    public static String obfuscateText( String queryText, Set<String> passwordParams )
    {
        Matcher matcher = PASSWORD_PATTERN.matcher( queryText );

        while ( matcher.find() )
        {
            String password = matcher.group( 1 ).trim();
            if ( password.charAt( 0 ) == '$' )
            {
                passwordParams.add( password.substring( 1 ) );
            }
            else if ( password.charAt( 0 ) == '{' )
            {
                passwordParams.add( password.substring( 1, password.length() - 1 ) );
            }
            else
            {
                queryText = queryText.replace( password, OBFUSCATED_LITERAL );
            }
        }

        return queryText;
    }

    public static MapValue obfuscateParams( MapValue queryParameters, Set<String> passwordParams )
    {
        for ( String passwordKey : passwordParams )
        {
            queryParameters = queryParameters.updatedWith( passwordKey, OBFUSCATED );
        }
        return queryParameters;
    }
}
