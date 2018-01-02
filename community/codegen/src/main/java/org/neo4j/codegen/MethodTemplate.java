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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.neo4j.codegen.Parameter.NO_PARAMETERS;
import static org.neo4j.codegen.TypeReference.NO_TYPES;
import static org.neo4j.codegen.TypeReference.typeReference;

public class MethodTemplate
{
    public static Builder method( Class<?> returnType, String name, Parameter... parameters )
    {
        return method( typeReference( returnType ), name, parameters );
    }

    public static Builder method( final TypeReference returnType, final String name, Parameter... parameters )
    {
        try
        {
            return new Builder( parameters )
            {
                @Override
                public MethodTemplate build()
                {
                    return buildMethod( this, returnType, name );
                }

                @Override
                MethodDeclaration.Builder declaration()
                {
                    return MethodDeclaration.method( returnType, name, parameters );
                }
            };
        }
        catch ( IllegalArgumentException | NullPointerException e )
        {
            throw new IllegalArgumentException( "Invalid signature for " + name + ": " + e.getMessage(), e );
        }
    }

    public static ConstructorBuilder constructor( Parameter... parameters )
    {
        try
        {
            return new ConstructorBuilder( parameters );
        }
        catch ( IllegalArgumentException | NullPointerException e )
        {
            throw new IllegalArgumentException( "Invalid constructor signature: " + e.getMessage(), e );
        }
    }

    public static class ConstructorBuilder extends Builder
    {
        ConstructorBuilder( Parameter[] parameters )
        {
            super( parameters );
        }

        public Builder invokeSuper( ExpressionTemplate... parameters )
        {
            return expression( ExpressionTemplate.invokeSuperConstructor( parameters ) );
        }

        @Override
        public MethodTemplate build()
        {
            return buildConstructor( this );
        }

        @Override
        MethodDeclaration.Builder declaration()
        {
            return MethodDeclaration.constructor( parameters );
        }
    }

    public static abstract class Builder
    {
        final Parameter[] parameters;
        private final Map<String,TypeReference> locals = new HashMap<>();
        private final List<Statement> statements = new ArrayList<>();

        Builder( Parameter[] parameters )
        {
            if ( parameters == null || parameters.length == 0 )
            {
                this.parameters = NO_PARAMETERS;
            }
            else
            {
                this.parameters = parameters.clone();
            }
            for ( int i = 0; i < this.parameters.length; i++ )
            {
                Parameter parameter = requireNonNull( this.parameters[i], "Parameter " + i );
                if ( null != locals.put( parameter.name(), parameter.type() ) )
                {
                    throw new IllegalArgumentException( "Duplicate parameters named \"" + parameter.name() + "\"." );
                }
            }
        }

        public abstract MethodTemplate build();

        public Builder expression( ExpressionTemplate expression )
        {
            statements.add( Statement.expression( expression ) );
            return this;
        }

        public Builder put( ExpressionTemplate target, Class<?> fieldType, String fieldName,
                ExpressionTemplate expression )
        {
            return put( target, typeReference( fieldType ), fieldName, expression );
        }

        public Builder put( ExpressionTemplate target, TypeReference fieldType, String fieldName,
                ExpressionTemplate expression )
        {
            statements.add( Statement.put( target, Lookup.field( fieldType, fieldName ), expression ) );
            return this;
        }

        public Builder returns( ExpressionTemplate value )
        {
            statements.add( Statement.returns( value ) );
            return this;
        }

        abstract MethodDeclaration.Builder declaration();
    }

    public TypeReference returnType()
    {
        return returnType;
    }

    public String name()
    {
        return name;
    }

    public TypeReference[] parameterTypes()
    {
        if ( parameters.length == 0 )
        {
            return NO_TYPES;
        }
        TypeReference[] result = new TypeReference[parameters.length];
        for ( int i = 0; i < result.length; i++ )
        {
            result[i] = parameters[i].type();
        }
        return result;
    }

    MethodDeclaration declaration( ClassHandle handle )
    {
        return declaration.build( handle );
    }

    void generate( CodeBlock generator )
    {
        for ( Statement statement : statements )
        {
            statement.generate( generator );
        }
    }

    private static MethodTemplate buildMethod( Builder builder, TypeReference returnType, String name )
    {
        return new MethodTemplate( builder, returnType, name );
    }

    private static MethodTemplate buildConstructor( Builder builder )
    {
        return new MethodTemplate( builder, TypeReference.VOID, "<init>" );
    }

    private final MethodDeclaration.Builder declaration;
    private final Parameter[] parameters;
    private final Statement[] statements;
    private final TypeReference returnType;
    private final String name;

    private MethodTemplate( Builder builder, TypeReference returnType, String name )
    {
        this.returnType = returnType;
        this.name = name;
        this.declaration = builder.declaration();
        this.parameters = builder.parameters;
        this.statements = builder.statements.toArray( new Statement[builder.statements.size()] );
    }
}
