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

public abstract class ExpressionTemplate
{
    public static ExpressionTemplate self()
    {
        return new ExpressionTemplate()
        {
            @Override
            void templateAccept( CodeBlock method, ExpressionVisitor visitor )
            {
                visitor.load( method.clazz.handle(), "this" );
            }
        };
    }

    /** invoke a static method or constructor */
    public static ExpressionTemplate invoke( final MethodReference method, final ExpressionTemplate... arguments )
    {
        // Try to materialize this expression early
        Expression[] materialized = tryMaterialize( arguments );
        if ( materialized != null )
        {
            return Expression.invoke( method, materialized );
        }
        // some part needs reference to the method context, so this expression will transitively need it
        return new ExpressionTemplate()
        {
            @Override
            protected void templateAccept( CodeBlock generator, ExpressionVisitor visitor )
            {
                visitor.invoke( method, materialize( generator, arguments ) );
            }
        };
    }

    /** invoke an instance method */
    public static ExpressionTemplate invoke( final ExpressionTemplate target, final MethodReference method,
            final ExpressionTemplate... arguments )
    {
        if ( target instanceof Expression )
        {
            Expression[] materialized = tryMaterialize( arguments );
            if ( materialized != null )
            {
                return Expression.invoke( (Expression) target, method, materialized );
            }
        }
        return new ExpressionTemplate()
        {
            @Override
            protected void templateAccept( CodeBlock generator, ExpressionVisitor visitor )
            {
                visitor.invoke( target.materialize( generator ), method, materialize( generator, arguments ) );
            }
        };
    }

    /** load a local variable */
    public static ExpressionTemplate load( final String name )
    {
        return new ExpressionTemplate()
        {
            @Override
            protected void templateAccept( CodeBlock method, ExpressionVisitor visitor )
            {
                visitor.load( method.local( name ), name );
            }
        };
    }

    public static ExpressionTemplate get( ExpressionTemplate target, Class<?> fieldType, String fieldName )
    {
        return get( target, typeReference( fieldType ), fieldName );
    }

    /** instance field */
    public static ExpressionTemplate get( ExpressionTemplate target, TypeReference fieldType, String fieldName )
    {
        return get( target, Lookup.field( fieldType, fieldName ) );
    }

    /** instance field */
    public static ExpressionTemplate get( final ExpressionTemplate target, final FieldReference field )
    {
        if ( target instanceof Expression )
        {
            return Expression.get( (Expression) target, field );
        }
        return new ExpressionTemplate()
        {
            @Override
            void templateAccept( CodeBlock method, ExpressionVisitor visitor )
            {
                visitor.getField( target.materialize( method ), field );
            }
        };
    }

    /** static field from the class that will host this expression */
    public static ExpressionTemplate get( TypeReference fieldType, String fieldName )
    {
        return get( Lookup.field( fieldType, fieldName ) );
    }

    /** instance field */
    public static ExpressionTemplate get( final ExpressionTemplate target, final Lookup<FieldReference> field )
    {
        return new ExpressionTemplate()
        {
            @Override
            void templateAccept( CodeBlock method, ExpressionVisitor visitor )
            {
                visitor.getField( target.materialize( method ), field.lookup( method ) );
            }
        };
    }

    /** static field */
    public static ExpressionTemplate get( final Lookup<FieldReference> field )
    {
        return new ExpressionTemplate()
        {
            @Override
            void templateAccept( CodeBlock method, ExpressionVisitor visitor )
            {
                visitor.getStatic( field.lookup( method ) );
            }
        };
    }

    Expression materialize( final CodeBlock method )
    {
        return new Expression()
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                ExpressionTemplate.this.templateAccept( method, visitor );
            }
        };
    }

    abstract void templateAccept( CodeBlock method, ExpressionVisitor visitor );

    private static Expression[] tryMaterialize( ExpressionTemplate[] templates )
    {
        if ( templates instanceof Expression[] )
        {
            return (Expression[]) templates;
        }
        Expression[] materialized = new Expression[templates.length];
        for ( int i = 0; i < materialized.length; i++ )
        {
            if ( templates[i] instanceof Expression )
            {
                materialized[i] = (Expression) templates[i];
            }
            else
            {
                return null;
            }
        }
        return materialized;
    }

    static Expression[] materialize( CodeBlock method, ExpressionTemplate[] templates )
    {
        Expression[] expressions = new Expression[templates.length];
        for ( int i = 0; i < expressions.length; i++ )
        {
            expressions[i] = templates[i].materialize( method );
        }
        return expressions;
    }

    static ExpressionTemplate invokeSuperConstructor( final ExpressionTemplate[] parameters )
    {
        // TODO: we need to get the parameter types in here eventually...
        return new ExpressionTemplate()
        {
            @Override
            void templateAccept( CodeBlock method, ExpressionVisitor visitor )
            {
                visitor.invoke( Expression.SUPER,
                        new MethodReference( method.clazz.handle().parent(), "<init>" ),
                        materialize( method, parameters ) );
            }
        };
    }
}
