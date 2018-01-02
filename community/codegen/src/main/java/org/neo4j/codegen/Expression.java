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

import static org.neo4j.codegen.TypeReference.typeReference;

public abstract class Expression extends ExpressionTemplate
{
    static final Expression SUPER = new Expression()
    {
        @Override
        public void accept( ExpressionVisitor visitor )
        {
            visitor.loadThis( "super" );
        }
    };

    public abstract void accept( ExpressionVisitor visitor );

    public static Expression gt( final Expression lhs, final Expression rhs )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.gt( lhs, rhs );
            }
        };
    }

    public static Expression or( final Expression lhs, final Expression rhs )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.or( lhs, rhs );
            }
        };
    }

    public static Expression eq( final Expression lhs, final Expression rhs )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.eq( lhs, rhs );
            }
        };
    }

    static Expression load( final TypeReference type, final String name )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.load( type, name );
            }
        };
    }

    public static Expression add( final Expression lhs, final Expression rhs )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.add( lhs, rhs );
            }
        };
    }

    public static Expression sub( final Expression lhs, final Expression rhs )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.sub( lhs, rhs );
            }
        };
    }

    public static Expression constant( final Object value )
    {
        if ( !(value == null ||
               value instanceof String ||
               value instanceof Long ||
               value instanceof Integer ||
               value instanceof Double ||
               value instanceof Boolean) )
        {
            throw new IllegalArgumentException( "Not a valid constant!" );
        }
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.constant( value );
            }
        };
    }

    /** get instance field */
    public static Expression get( final Expression target, final FieldReference field )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.getField( target, field );
            }
        };
    }

    /** get static field */
    public static Expression get( final FieldReference field )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.getStatic( field );
            }
        };
    }

    public static Expression ternary( final Expression test, final Expression onTrue, final Expression onFalse )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.ternary( test, onTrue, onFalse );
            }
        };
    }

    public static Expression invoke( final Expression target, final MethodReference method,
            final Expression... arguments )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.invoke( target, method, arguments );
            }
        };
    }

    public static Expression invoke( final MethodReference method, final Expression... parameters )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.invoke( method, parameters );
            }
        };
    }

    public static Expression newInstance( Class<?> type )
    {
        return newInstance( typeReference( type ) );
    }

    public static Expression newInstance( final TypeReference type )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.newInstance( type );
            }
        };
    }

    public static Expression not( final Expression expression )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.not( expression );
            }
        };
    }

    @Override
    Expression materialize( CodeBlock method )
    {
        return this;
    }

    @Override
    void templateAccept( CodeBlock method, ExpressionVisitor visitor )
    {
        throw new UnsupportedOperationException( "simple expressions should not be invoked as templates" );
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder().append( "Expression[" );
        accept( new ExpressionToString( result ) );
        return result.append( ']' ).toString();
    }
}
