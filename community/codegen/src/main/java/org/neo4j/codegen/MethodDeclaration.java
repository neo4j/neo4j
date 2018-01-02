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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.neo4j.codegen.TypeReference.NO_TYPES;
import static org.neo4j.codegen.TypeReference.typeReference;

public abstract class MethodDeclaration
{
    public static Builder method( Class<?> returnType, String name, Parameter... parameters )
    {
        return method( typeReference( returnType ), name, parameters );
    }

    public static Builder method( final TypeReference returnType, final String name, Parameter... parameters )
    {
        return new Builder( parameters )
        {
            @Override
            MethodDeclaration build( TypeReference owner )
            {
                return method( owner, returnType, name, parameters, exceptions(), typeParameters() );
            }
        };
    }

    static Builder constructor( Parameter... parameters )
    {
        return new Builder( parameters )
        {
            @Override
            MethodDeclaration build( TypeReference owner )
            {
                return constructor( owner, parameters, exceptions(), typeParameters() );
            }
        };
    }

    public List<TypeParameter> typeParameters()
    {
        return unmodifiableList( asList( typeParameters ) );
    }

    public List<TypeReference> throwsList()
    {
        return unmodifiableList( asList( exceptions ) );
    }

    public static abstract class Builder
    {
        private LinkedHashMap<String,TypeReference.Bound> typeParameters;

        public Builder parameterizedWith( String name, TypeReference.Bound bound )
        {
            if ( typeParameters == null )
            {
                typeParameters = new LinkedHashMap<>();
            }
            else if ( typeParameters.containsKey( name ) )
            {
                throw new IllegalArgumentException( name + " defined twice" );
            }
            typeParameters.put( name, bound );
            return this;
        }

        public Builder throwsException( Class<?> type )
        {
            return throwsException( TypeReference.typeReference( type ) );
        }

        public Builder throwsException( TypeReference type )
        {
            if ( exceptions == null )
            {
                exceptions = new ArrayList<>();
            }
            exceptions.add( type );
            return this;
        }

        abstract MethodDeclaration build( TypeReference owner );

        final Parameter[] parameters;
        private List<TypeReference> exceptions;

        private Builder( Parameter[] parameters )
        {
            this.parameters = parameters;
        }

        TypeReference[] exceptions()
        {
            return exceptions == null ? NO_TYPES : exceptions.toArray( new TypeReference[exceptions.size()] );
        }

        TypeParameter[] typeParameters()
        {
            if ( typeParameters == null )
            {
                return TypeParameter.NO_PARAMETERS;
            }
            else
            {
                TypeParameter[] result = new TypeParameter[typeParameters.size()];
                int i = 0;
                for ( Map.Entry<String,TypeReference.Bound> entry : typeParameters.entrySet() )
                {
                    result[i++] = new TypeParameter( entry.getKey(), entry.getValue() );
                }
                return result;
            }
        }
    }

    private final TypeReference owner;
    private final Parameter[] parameters;
    private final TypeReference[] exceptions;
    private final TypeParameter[] typeParameters;

    MethodDeclaration( TypeReference owner, Parameter[] parameters, TypeReference[] exceptions,
            TypeParameter[] typeParameters )
    {
        this.owner = owner;
        this.parameters = parameters;
        this.exceptions = exceptions;
        this.typeParameters = typeParameters;
    }

    public abstract boolean isConstructor();

    public boolean isStatic()
    {
        return false;
    }

    public TypeReference declaringClass()
    {
        return owner;
    }

    public abstract TypeReference returnType();

    public abstract String name();

    public Parameter[] parameters()
    {
        return parameters;
    }

    static MethodDeclaration method( TypeReference owner, final TypeReference returnType, final String name,
            Parameter[] parameters, TypeReference[] exceptions, TypeParameter[] typeParameters )
    {
        return new MethodDeclaration( owner, parameters, exceptions, typeParameters )
        {
            @Override
            public boolean isConstructor()
            {
                return false;
            }

            @Override
            public TypeReference returnType()
            {
                return returnType;
            }

            @Override
            public String name()
            {
                return name;
            }
        };
    }

    static MethodDeclaration constructor( TypeReference owner, Parameter[] parameters, TypeReference[] exceptions,
            TypeParameter[] typeParameters )
    {
        return new MethodDeclaration( owner, parameters, exceptions, typeParameters )
        {
            @Override
            public boolean isConstructor()
            {
                return true;
            }

            @Override
            public TypeReference returnType()
            {
                return TypeReference.VOID;
            }

            @Override
            public String name()
            {
                return "<init>";
            }
        };
    }

    public static class TypeParameter
    {
        static final TypeParameter[] NO_PARAMETERS = {};

        final String name;
        final TypeReference.Bound bound;

        TypeParameter( String name, TypeReference.Bound bound )
        {
            this.name = name;
            this.bound = bound;
        }

        public String name()
        {
            return name;
        }

        public TypeReference extendsBound()
        {
            return bound.extendsBound();
        }

        public TypeReference superBound()
        {
            return bound.superBound();
        }
    }
}
