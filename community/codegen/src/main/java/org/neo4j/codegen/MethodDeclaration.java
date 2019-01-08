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
package org.neo4j.codegen;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.neo4j.codegen.Parameter.param;
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
                return method( owner, returnType, name, parameters, exceptions(), modifiers(), typeParameters() );
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
                return constructor( owner, parameters, exceptions(), modifiers(), typeParameters() );
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

    public abstract static class Builder
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

        public Builder modifiers( int modifiers )
        {
            this.modifiers = modifiers;
            return this;
        }

        public int modifiers()
        {
            return modifiers;
        }

        abstract MethodDeclaration build( TypeReference owner );

        final Parameter[] parameters;
        private List<TypeReference> exceptions;
        private int modifiers = Modifier.PUBLIC;

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
    private final int modifiers;

    MethodDeclaration( TypeReference owner, Parameter[] parameters, TypeReference[] exceptions,
            int modifiers, TypeParameter[] typeParameters )
    {
        this.owner = owner;
        this.parameters = parameters;
        this.exceptions = exceptions;
        this.modifiers = modifiers;
        this.typeParameters = typeParameters;
    }

    public abstract boolean isConstructor();

    public boolean isStatic()
    {
        return Modifier.isStatic( modifiers );
    }

    public boolean isGeneric()
    {
        if ( returnType().isGeneric() || typeParameters.length != 0 )
        {
            return true;
        }
        for ( Parameter parameter : parameters )
        {
            if ( parameter.type().isGeneric() )
            {
                return true;
            }
        }

        return false;
    }

    public TypeReference declaringClass()
    {
        return owner;
    }

    public int modifiers()
    {
        return modifiers;
    }

    public abstract TypeReference returnType();

    public abstract String name();

    public Parameter[] parameters()
    {
        return parameters;
    }

    public MethodDeclaration erased()
    {
        Map<String,TypeReference> table = new HashMap<>();
        for ( TypeParameter parameter : typeParameters )
        {
            table.put( parameter.name(), parameter.extendsBound() );
        }

        TypeReference newReturnType = erase( returnType(), table );
        Parameter[] newParameters = new Parameter[this.parameters.length];
        for ( int i = 0; i < parameters.length; i++ )
        {
            Parameter parameter = parameters[i];
            TypeReference erasedType = erase( parameter.type(), table );
            newParameters[i] = param( erasedType, parameter.name() );
        }
        TypeReference[] newExceptions = new TypeReference[exceptions.length];
        for ( int i = 0; i < exceptions.length; i++ )
        {
            newExceptions[i] = erase( exceptions[i], table );
        }
        String newName = name();
        boolean newIsConstrucor = isConstructor();

        return methodDeclaration( owner, newReturnType, newParameters, newExceptions, newName, newIsConstrucor,
                modifiers, typeParameters );
    }

    private TypeReference erase( TypeReference reference, Map<String,TypeReference> table )
    {
        TypeReference erasedReference = table.get( reference.fullName() );

        return erasedReference != null ? erasedReference : reference;
    }

    static MethodDeclaration method( TypeReference owner, final TypeReference returnType, final String name,
            Parameter[] parameters, TypeReference[] exceptions, int modifiers, TypeParameter[] typeParameters )
    {
        return methodDeclaration( owner, returnType, parameters, exceptions, name, false, modifiers, typeParameters );
    }

    static MethodDeclaration constructor( TypeReference owner, Parameter[] parameters, TypeReference[] exceptions,
            int modifiers, TypeParameter[] typeParameters )
    {
        return methodDeclaration( owner, TypeReference.VOID, parameters, exceptions, "<init>", true, modifiers,
                typeParameters );
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

    private static MethodDeclaration methodDeclaration( TypeReference owner, final TypeReference returnType,
            final Parameter[] parameters, final TypeReference[] exceptions, final String name,
            final boolean isConstrucor, int modifiers, TypeParameter[] typeParameters )
    {
        return new MethodDeclaration( owner, parameters, exceptions, modifiers, typeParameters )
        {
            @Override
            public boolean isConstructor()
            {
                return isConstrucor;
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
}
