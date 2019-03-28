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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.values.AnyValue;

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
        String packageName = "";
        String name;
        TypeReference declaringTypeReference = null;

        Class<?> innerType = type.isArray() ? type.getComponentType() : type;

        if ( innerType.isPrimitive() )
        {
            name = innerType.getName();
            switch ( name )
            {
            case "boolean":
                return type.isArray() ? BOOLEAN_ARRAY : BOOLEAN;
            case "int":
                return type.isArray() ? INT_ARRAY : INT;
            case "long":
                return type.isArray() ? LONG_ARRAY : LONG;
            case "double":
                return type.isArray() ? DOUBLE_ARRAY : DOUBLE;
            default:
                // continue through the normal path
            }
        }
        else
        {
            packageName = innerType.getPackage().getName();
            Class<?> declaringClass = innerType.getDeclaringClass();
            if ( declaringClass != null )
            {
                declaringTypeReference = typeReference( declaringClass );
            }
            name = innerType.getSimpleName();
        }
        return new TypeReference( packageName, name, type.isPrimitive(), type.isArray(), false,
                declaringTypeReference, type.getModifiers() );
    }

    public static TypeReference typeParameter( String name )
    {
        return new TypeReference( "", name, false, false, true, null, Modifier.PUBLIC );
    }

    public static TypeReference arrayOf( TypeReference type )
    {
        return new TypeReference( type.packageName, type.name, false, true, false, type.declaringClass, type.modifiers );
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
        return new TypeReference( base.packageName, base.name, false, base.isArray(), false,
                base.declaringClass,
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

    public static TypeReference toBoxedType( TypeReference in )
    {
        switch ( in.fullName() )
        {
        case "byte":
            return TypeReference.typeReference( Byte.class );
        case "short":
            return TypeReference.typeReference( Short.class );
        case "int":
            return TypeReference.typeReference( Integer.class );
        case "long":
            return TypeReference.typeReference( Long.class );
        case "char":
            return TypeReference.typeReference( Character.class );
        case "boolean":
            return TypeReference.typeReference( Boolean.class );
        case "float":
            return TypeReference.typeReference( Float.class );
        case "double":
            return TypeReference.typeReference( Double.class );
        default:
            return in;
        }
    }

    private final String packageName;
    private final String name;
    private final TypeReference[] parameters;
    private final boolean isPrimitive;
    private final boolean isArray;
    private final boolean isTypeParameter;
    private final TypeReference declaringClass;
    private final int modifiers;

    public static final TypeReference VOID =
            new TypeReference( "", "void", true, false, false, null, void.class.getModifiers() );
    public static final TypeReference OBJECT =
            new TypeReference( "java.lang", "Object", false, false, false, null, Object.class.getModifiers() );
    public static final TypeReference BOOLEAN =
            new TypeReference( "", "boolean", true, false, false, null, boolean.class.getModifiers() );
    public static final TypeReference INT =
            new TypeReference( "", "int", true, false, false, null, int.class.getModifiers() );
    public static final TypeReference LONG =
            new TypeReference( "", "long", true, false, false, null, long.class.getModifiers() );
    public static final TypeReference DOUBLE =
            new TypeReference( "", "double", true, false, false, null, double.class.getModifiers() );
    public static final TypeReference BOOLEAN_ARRAY =
            new TypeReference( "", "boolean", false, true, false, null, boolean.class.getModifiers() );
    public static final TypeReference INT_ARRAY =
            new TypeReference( "", "int", false, true, false, null, int.class.getModifiers() );
    public static final TypeReference LONG_ARRAY =
            new TypeReference( "", "long", false, true, false, null, long.class.getModifiers() );
    public static final TypeReference DOUBLE_ARRAY =
            new TypeReference( "", "double", false, true, false, null, double.class.getModifiers() );
    public static final TypeReference VALUE =
            new TypeReference( "org.neo4j.values", "AnyValue", false, false, false, null, AnyValue.class.getModifiers() );
    static final TypeReference[] NO_TYPES = new TypeReference[0];

    TypeReference( String packageName, String name, boolean isPrimitive, boolean isArray,
            boolean isTypeParameter, TypeReference declaringClass, int modifiers, TypeReference... parameters )
    {
        this.packageName = packageName;
        this.name = name;
        this.isPrimitive = isPrimitive;
        this.isArray = isArray;
        this.isTypeParameter = isTypeParameter;
        this.declaringClass = declaringClass;
        this.modifiers = modifiers;
        this.parameters = parameters;
    }

    public String packageName()
    {
        return packageName;
    }

    public String name()
    {
        return name;
    }

    public String simpleName()
    {
        return isArray ? name + "[]" : name;
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
        return List.of( parameters );
    }

    public String fullName()
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
        return declaringClass != null;
    }

    public List<TypeReference> declaringClasses()
    {
        LinkedList<TypeReference> parents = new LinkedList<>(  );
        TypeReference parent = declaringClass;
        while ( parent != null )
        {
            parents.addFirst( parent );
            parent = parent.declaringClass;
        }
        return parents;
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
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        TypeReference reference = (TypeReference) o;

        if ( isPrimitive != reference.isPrimitive )
        {
            return false;
        }
        if ( isArray != reference.isArray )
        {
            return false;
        }
        if ( isTypeParameter != reference.isTypeParameter )
        {
            return false;
        }
        if ( modifiers != reference.modifiers )
        {
            return false;
        }
        if ( packageName != null ? !packageName.equals( reference.packageName ) : reference.packageName != null )
        {
            return false;
        }
        if ( name != null ? !name.equals( reference.name ) : reference.name != null )
        {
            return false;
        }
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if ( !Arrays.equals( parameters, reference.parameters ) )
        {
            return false;
        }
        return declaringClass != null ? declaringClass.equals( reference.declaringClass )
                                          : reference.declaringClass == null;

    }

    @Override
    public int hashCode()
    {
        int result = packageName != null ? packageName.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + Arrays.hashCode( parameters );
        result = 31 * result + (isPrimitive ? 1 : 0);
        result = 31 * result + (isArray ? 1 : 0);
        result = 31 * result + (isTypeParameter ? 1 : 0);
        result = 31 * result + (declaringClass != null ? declaringClass.hashCode() : 0);
        result = 31 * result + modifiers;
        return result;
    }

    public String baseName()
    {
        return writeBaseType( new StringBuilder() ).toString();
    }

    @Override
    public String toString()
    {
        return writeTo( new StringBuilder().append( "TypeReference[" ) ).append( ']' ).toString();
    }

    StringBuilder writeTo( StringBuilder result )
    {
        writeBaseType( result );
        if ( isArray )
        {
            result.append( "[]" );
        }
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

    StringBuilder writeBaseType( StringBuilder result )
    {
        if ( !packageName.isEmpty() )
        {
            result.append( packageName ).append( '.' );
        }
        List<TypeReference> parents = declaringClasses();
        for ( TypeReference parent : parents )
        {
            result.append( parent.name ).append( '.' );
        }
        result.append( name );
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
