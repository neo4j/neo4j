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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory;
import org.neo4j.cypher.internal.ast.factory.SimpleEither;

public class AliasName<PARAMETER>
{
    ASTExceptionFactory exceptionFactory;
    Token start;
    List<String> names = new ArrayList<>();
    PARAMETER parameter;

    public AliasName( ASTExceptionFactory exceptionFactory, Token token )
    {
        this.exceptionFactory = exceptionFactory;
        this.start = token;
        this.names.add( token.image );
    }

    public AliasName( ASTExceptionFactory exceptionFactory, PARAMETER parameter )
    {
        this.exceptionFactory = exceptionFactory;
        this.parameter = parameter;
    }

    public void add( Token token )
    {
        names.add( token.image );
    }

    public SimpleEither<String,PARAMETER> getRemoteAliasName() throws Exception
    {
        if ( parameter != null )
        {
            return SimpleEither.right( parameter );
        }
        else
        {
            if ( names.size() > 1 )
            {
                throw exceptionFactory.syntaxException(
                        new ParseException( ASTExceptionFactory.invalidDotsInRemoteAliasName( String.join( ".", names ) ) ),
                        start.beginOffset,
                        start.beginLine,
                        start.beginColumn );
            }
            else
            {
                return SimpleEither.left( names.get( 0 ) );
            }
        }
    }

    public SimpleEither<String,PARAMETER> getLocalAliasName()
    {
        if ( parameter != null )
        {
            return SimpleEither.right( parameter );
        }
        else
        {
            return SimpleEither.left( String.join( ".", names ) );
        }
    }
}
