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
package org.neo4j.codegen;

abstract class Statement
{
    static Statement expression( final ExpressionTemplate expression )
    {
        return new Statement()
        {
            @Override
            void generate( CodeBlock method )
            {
                method.expression( expression.materialize( method ) );
            }
        };
    }

    public static Statement returns( final ExpressionTemplate expression )
    {
        return new Statement()
        {
            @Override
            void generate( CodeBlock method )
            {
                method.returns( expression.materialize( method ) );
            }
        };
    }

    abstract void generate( CodeBlock method );

    public static Statement put( final ExpressionTemplate target, final Lookup<FieldReference> field,
            final ExpressionTemplate expression )
    {
        return new Statement()
        {
            @Override
            void generate( CodeBlock method )
            {
                method.put( target.materialize( method ), field.lookup( method ), expression.materialize( method ) );
            }
        };
    }
}
