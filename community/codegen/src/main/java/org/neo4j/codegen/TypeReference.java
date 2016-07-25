/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public class TypeReference
{
    public static Bound extending( Class<?> type )
    {
        return extending( typeReference( type ) );
    }

    public static Bound extending( final TypeReference type )
    {
        return new Bound( type )
        {
            @Override
            public TypeReference extendsBound()
            {
                return type;
            }

            @Override
            public TypeReference superBound()
            {
                return null;
            }
        };
    }

    public static TypeReference typeReference( Class<?> type )
    {
        if ( type == void.class )
        {
            return VOID;
        }
        if ( type == Object.class )
        {
            return OBJECT;
        }
        String packageName = "", simpleName, declaringClassName = "";
        if ( type.isArray() )
        {
            simpleName = type.getComponentType().getCanonicalName() + "[]";
        }
        else if (type.isPrimitive())
        {
            simpleName = type.getName();
        }
        else
        {
            packageName = type.getPackage().getName();
            String canonicalName = type.getCanonicalName();
            Class<?> declaringClass = type.getDeclaringClass();
            if ( declaringClass != null)
            {
                declaringClassName = declaringClass.getSimpleName();
                simpleName = canonicalName.substring( packageName.length() + declaringClassName.length() + 2 );
            }
            else
            {
                simpleName = canonicalName.substring( packageName.length() + 1 );
            }
        }
        return new TypeReference( packageName, simpleName, type.isPrimitive(), type.isArray(), false,
                declaringClassName, type.getModifiers() );
    }

    public static TypeReference typeParameter( String name )
    {
        return new TypeReference( "", name, false, false, true, "", Modifier.PUBLIC );
    }

    public static TypeReference parameterizedType( Class<?> base, Class<?>... parameters )
    {
        return parameterizedType( typeReference( base ), typeReferences( parameters ) );
    }

    public static TypeReference parameterizedType( Class<?> base, TypeReference... parameters )
    {
        return parameterizedType( typeReference( base ), parameters );
    }

    public static TypeReference parameterizedType( TypeReference base, TypeReference... parameters )
    {
        return new TypeReference( base.packageName, base.simpleName, false, base.isArray(), false,
                base.declaringClassName,
                base.modifiers, parameters );
    }

    public static TypeReference[] typeReferences( Class<?> first, Class<?>[] more )
    {
        TypeReference[] result = new TypeReference[more.length + 1];
        result[0] = typeReference( first );
        for ( int i = 0; i < more.length; i++ )
        {
            result[i + 1] = typeReference( more[i] );
        }
        return result;
    }

    public static TypeReference[] typeReferences( Class<?>[] types )
    {
        TypeReference[] result = new TypeReference[types.length];
        for ( int i = 0; i < result.length; i++ )
        {
            result[i] = typeReference( types[i] );
        }
        return result;
    }

    private final String packageName;
    private final String simpleName;
    private final TypeReference[] parameters;
    private final boolean isPrimitive;
    private final boolean isArray;
    private final boolean isTypeParameter;
    private final String declaringClassName;
    private final int modifiers;

    public static final TypeReference VOID = new TypeReference( "", "void", true, false, false, "", void.class.getModifiers() ),
            OBJECT = new TypeReference( "java.lang", "Object", false, false, false, "", Object.class.getModifiers() );
    static final TypeReference[] NO_TYPES = new TypeReference[0];

    TypeReference( String packageName, String simpleName, boolean isPrimitive, boolean isArray,
            boolean isTypeParameter, String declaringClassName, int modifiers, TypeReference... parameters )
    {
        this.packageName = packageName;
        this.simpleName = simpleName;
        this.isPrimitive = isPrimitive;
        this.isArray = isArray;
        this.isTypeParameter = isTypeParameter;
        this.declaringClassName = declaringClassName;
        this.modifiers = modifiers;
        this.parameters = parameters;
    }

    public String packageName()
    {
        return packageName;
    }

    public String simpleName()
    {
        return simpleName;
    }

    public boolean isPrimitive()
    {
        return isPrimitive;
    }

    public boolean isTypeParameter()
    {
        return isTypeParameter;
    }

    public boolean isGeneric()
    {
        return parameters == null || parameters.length > 0;
    }

    public List<TypeReference> parameters()
    {
        return unmodifiableList( asList( parameters ) );
    }

    public String name()
    {
        return writeTo( new StringBuilder() ).toString();
    }

    public boolean isArray()
    {
        return isArray;
    }

    public boolean isVoid()
    {
        return this == VOID;
    }

    public boolean isInnerClass()
    {
        return !declaringClassName.isEmpty();
    }

    public String declaringClassName()
    {
        return declaringClassName;
    }

    public int modifiers()
    {
        return modifiers;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !(o instanceof TypeReference) )
        {
            return false;
        }
        TypeReference that = (TypeReference) o;
        return simpleName.equals( that.simpleName ) &&
               packageName.equals( that.packageName ) &&
               Arrays.equals( parameters, that.parameters );
    }

    @Override
    public int hashCode()
    {
        int result = packageName.hashCode();
        result = 31 * result + simpleName.hashCode();
        result = 31 * result + Arrays.hashCode( parameters );
        return result;
    }

    @Override
    public String toString()
    {
        return writeTo( new StringBuilder().append( "TypeReference[" ) ).append( ']' ).toString();
    }

    StringBuilder writeTo( StringBuilder result )
    {
        if ( !packageName.isEmpty() )
        {
            result.append( packageName ).append( '.' );
        }
        if ( !declaringClassName.isEmpty() )
        {
            result.append( declaringClassName ).append( '.' );
        }
        result.append( simpleName );
        if ( !(parameters == null || parameters.length == 0) )
        {
            result.append( '<' );
            String sep = "";
            for ( TypeReference parameter : parameters )
            {
                parameter.writeTo( result.append( sep ) );
                sep = ",";
            }
            result.append( '>' );
        }
        return result;
    }

    public abstract static class Bound
    {
        private final TypeReference type;

        private Bound( TypeReference type )
        {
            this.type = type;
        }

        public abstract TypeReference extendsBound();

        public abstract TypeReference superBound();
    }
}
