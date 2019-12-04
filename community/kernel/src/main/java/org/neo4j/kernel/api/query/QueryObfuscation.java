/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static org.neo4j.values.storable.Values.stringValue;

class QueryObfuscation
{
    private static final Pattern PASSWORD_PATTERN = Pattern.compile(
            // call signature
            "(?:(?i)call)\\s+(?:dbms(?:\\.security)?\\.change(?:User)?Password|dbms\\.security\\.createUser)\\(\\s*" +
            // optional username parameter, in single, double quotes, or parametrized
            "(?:\\s*(?:'(?:(?<=\\\\)'|[^'])*'|\"(?:(?<=\\\\)\"|[^\"])*\"|[^,]*)\\s*,)?" +
            // password parameter, in single, double quotes, or parametrized
            "\\s*('(?:(?<=\\\\)'|[^'])*'|\"(?:(?<=\\\\)\"|[^\"])*\"|\\$\\w*|\\{\\w*})" );

    private static final TextValue OBFUSCATED = stringValue( "******" );
    private static final String OBFUSCATED_LITERAL = "'******'";

    static String obfuscateText( String queryText, Set<String> passwordParams )
    {
        Matcher matcher = PASSWORD_PATTERN.matcher( queryText );

        while ( matcher.find() )
        {
            String password = matcher.group( 1 ).trim();
            if ( password.charAt( 0 ) == '$' )
            {
                passwordParams.add( password.substring( 1 ) );
            }
            else
            {
                queryText = queryText.replace( password, OBFUSCATED_LITERAL );
            }
        }

        return queryText;
    }

    private static final Pattern SYSTEM_PASSWORD_PATTERN = Pattern.compile(
            // CREATE USER user SET PASSWORD
            // CREATE OR REPLACE USER user SET PASSWORD
            // CREATE USER user IF NOT EXISTS SET PASSWORD
            // ALTER USER user SET PASSWORD
            // ALTER CURRENT USER SET PASSWORD
            "^(?:(?:ALTER|CREATE)\\s+(?:CURRENT\\s+)?(?:OR\\s+REPLACE\\s+)?USER\\s+(?:(?:`)?\\w+(?:`)?\\s+)?(?:IF\\s+NOT\\s+EXISTS\\s+)?SET\\s+PASSWORD\\s+)" +
            // password can be in single, double quotes, or parametrized
            // FROM password TO password
            "(?:FROM\\s+)?((?:\\$\\w+)|(?:\"[^\"]*\")|(?:'[^']*'))(?:\\s+TO\\s+)?((?:\\$\\w+)|(?:\"[^\"]*\")|(?:'[^']*'))?",
            CASE_INSENSITIVE
        );

    static String obfuscateSystemCommand( String queryText, Set<String> passwordParams )
    {
        Matcher matcher = SYSTEM_PASSWORD_PATTERN.matcher( queryText );

        while ( matcher.find() )
        {
            String password = matcher.group( 1 ).trim();
            if ( password.charAt( 0 ) == '$' )
            {
                passwordParams.add( password.substring( 1 ) );
            }
            else
            {
                queryText = queryText.replace( password, OBFUSCATED_LITERAL );
            }
            String toPassword = matcher.group( 2 );
            if ( toPassword != null )
            {
                if ( toPassword.charAt( 0 ) == '$' )
                {
                    passwordParams.add( toPassword.substring( 1 ) );
                }
                else
                {
                    queryText = queryText.replace( toPassword, OBFUSCATED_LITERAL );
                }
            }
        }
        return queryText;
    }

    static MapValue obfuscateParams( MapValue queryParameters, Set<String> passwordParams )
    {
        for ( String passwordKey : passwordParams )
        {
            queryParameters = queryParameters.updatedWith( passwordKey, OBFUSCATED );
        }
        return queryParameters;
    }
}
