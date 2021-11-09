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
package org.neo4j.cypher.internal.parser.javacc;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ParseExceptions extends RuntimeException
{
    public static List<String> expected( int[][] expectedTokenSequences, String[] tokenImage )
    {
        Map<Integer,Long> tokenCount = Arrays.stream( expectedTokenSequences )
                                             .flatMapToInt( Arrays::stream )
                                             .boxed()
                                             .collect( Collectors.groupingBy( Function.identity(), Collectors.counting() ) );
        List<String> strings = processExpectedList( tokenCount, tokenImage );
        Collections.sort( strings );
        return strings;
    }

    public static List<String> processExpectedList( Map<Integer,Long> expectedTokens, String[] tokenImage )
    {
        long identifiers = expectedTokens.getOrDefault( CypherConstants.IDENTIFIER, 0L);
        long plusCount = expectedTokens.getOrDefault( CypherConstants.PLUS, 0L);
        long expressions = Math.min( identifiers, plusCount );
        if ( identifiers > 0 )
        {
            filterTokenSet( expectedTokens, IdentifierTokens.getIdentifierTokens(), identifiers );
        }
        if ( expressions > 0 )
        {
            filterTokenSet( expectedTokens, ExpressionTokens.getExpressionTokens(), expressions );
        }
        List<String> expectedMessage = expectedTokens.keySet().stream()
                                                     .map( token ->
                                                           {
                                                               String image = tokenImage[token];
                                                               return image.equals( "\"$\"" ) ? "a parameter" : image;
                                                           } )
                                                     .collect( Collectors.toList() );
        if ( identifiers - expressions > 0 )
        {
            expectedMessage.add( "an identifier" );
        }
        if ( expressions > 0 )
        {
            expectedMessage.add( "an expression" );
        }
        return expectedMessage;
    }

    private static Map<Integer,Long> filterTokenSet( Map<Integer,Long> expectedTokens, Set<Integer> tokens, long quantitiy )
    {
        for ( Integer token : tokens )
        {
            if ( expectedTokens.containsKey( token ) )
            {
                long newCount = expectedTokens.get( token ) - quantitiy;
                if ( newCount > 0 )
                {
                    expectedTokens.replace( token, newCount );
                }
                else
                {
                    expectedTokens.remove( token );
                }
            }
        }
        return expectedTokens;
    }
}
