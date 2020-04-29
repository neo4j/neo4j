/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.lang.reflect.Method;
import java.util.List;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Arrays.stream;
import static org.neo4j.codegen.TypeReference.typeReference;

public final class ByteCodeUtils
{
    private ByteCodeUtils()
    {
        throw new UnsupportedOperationException();
    }

    public static String byteCodeName( TypeReference reference )
    {
       return className( reference ).replaceAll( "\\.", "/" );
    }

    public static String className( TypeReference reference )
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "[".repeat( Math.max( 0, reference.arrayDepth() ) ) );
        if ( reference.arrayDepth() > 0 )
        {
            builder.append( 'L' );
        }
        if ( !reference.packageName().isEmpty() )
        {
            builder.append( reference.packageName() ).append( '.' );
        }

        for ( TypeReference parent : reference.declaringClasses() )
        {
            builder.append( parent.name() ).append( '$' );
        }
        builder.append( reference.name() );
        if ( reference.isArray() )
        {
            builder.append( ';' );
        }
        return builder.toString();
    }

    public static String outerName( TypeReference reference )
    {
        if ( !reference.isInnerClass() )
        {
            return null;
        }

        StringBuilder builder = new StringBuilder();
        if ( !reference.packageName().isEmpty() )
        {
            builder.append( reference.packageName().replaceAll( "\\.", "/" ) ).append( '/' );
        }
        builder.append( reference.simpleName() );

        return builder.toString();
    }

    public static String typeName( TypeReference reference )
    {
        StringBuilder builder = new StringBuilder();
        internalType( builder, reference, false );

        return builder.toString();
    }

    public static String desc( MethodDeclaration declaration )
    {
        return internalDesc( declaration.erased(), false );
    }

    public static String desc( MethodReference reference )
    {
        StringBuilder builder = new StringBuilder();
        builder.append( '(' );
        for ( TypeReference parameter : reference.parameters() )
        {
            internalType( builder, parameter, false );
        }
        builder.append( ')' );
        internalType( builder, reference.returns(), false );

        return builder.toString();
    }

    public static String signature( TypeReference reference )
    {
        if ( !reference.isGeneric() )
        {
            return null;
        }

        return internalSignature( reference );
    }

    public static String signature( MethodDeclaration declaration )
    {
        if ( !declaration.isGeneric() )
        {
            return null;
        }
        return internalDesc( declaration, true );
    }

    public static String[] exceptions( MethodDeclaration declaration )
    {

        List<TypeReference> throwsList = declaration.erased().throwsList();
        if ( throwsList.isEmpty() )
        {
            return null;
        }
        return throwsList.stream().map( ByteCodeUtils::byteCodeName ).toArray( String[]::new );
    }

    private static String internalDesc( MethodDeclaration declaration, boolean showErasure )
    {
        StringBuilder builder = new StringBuilder();
        List<MethodDeclaration.TypeParameter> typeParameters = declaration.typeParameters();
        if ( showErasure && !typeParameters.isEmpty() )
        {
            builder.append( '<' );
            for ( MethodDeclaration.TypeParameter typeParameter : typeParameters )
            {
                builder.append( typeParameter.name() ).append( ':' );
                internalType( builder, typeParameter.extendsBound(), true );
            }
            builder.append( '>' );
        }
        builder.append( '(' );
        for ( Parameter parameter : declaration.parameters() )
        {
            internalType( builder, parameter.type(), showErasure );
        }
        builder.append( ')' );
        internalType( builder, declaration.returnType(), showErasure );
        List<TypeReference> throwsList = declaration.throwsList();
        if ( showErasure && throwsList.stream().anyMatch( TypeReference::isTypeParameter ) )
        {
            builder.append( '^' );
            throwsList.forEach( t -> internalType( builder, t, false ) );
        }
        return builder.toString();
    }

    private static String internalSignature( TypeReference reference )
    {
        return internalType( new StringBuilder(), reference, true ).toString();
    }

    private static StringBuilder internalType( StringBuilder builder, TypeReference reference,
            boolean showErasure )
    {
        String name = reference.name();
        builder.append( "[".repeat( Math.max( 0, reference.arrayDepth() ) ) );
        switch ( name )
        {
        case "int":
            builder.append( 'I' );
            break;
        case "long":
            builder.append( 'J' );
            break;
        case "byte":
            builder.append( 'B' );
            break;
        case "short":
            builder.append( 'S' );
            break;
        case "char":
            builder.append( 'C' );
            break;
        case "float":
            builder.append( 'F' );
            break;
        case "double":
            builder.append( 'D' );
            break;
        case "boolean":
            builder.append( 'Z' );
            break;
        case "void":
            builder.append( 'V' );
            break;

        default:
            if ( reference.isTypeParameter() )
            {
                builder.append( 'T' ).append( name );
            }
            else
            {
                builder.append( 'L' );
                String packageName = reference.packageName().replaceAll( "\\.", "\\/" );
                if ( !packageName.isEmpty() )
                {
                    builder.append( packageName ).append( '/' );
                }
                for ( TypeReference parent : reference.declaringClasses() )
                {
                    builder.append( parent.name() ).append( '$' );
                }
                builder.append( name.replaceAll( "\\.", "\\/" ) );
            }

            List<TypeReference> parameters = reference.parameters();
            if ( showErasure && !parameters.isEmpty() )
            {
                builder.append( '<' );
                parameters.forEach( p -> internalType( builder, p, true ) );
                builder.append( '>' );
            }
            builder.append( ';' );

        }
        return builder;
    }

    public static void assertMethodExists( MethodReference methodReference )
    {
        Class<?> clazz;
        try
        {
            clazz = asClass( methodReference.owner() );
        }
        catch ( AssertionError e )
        {
            //if the class doesn't exist here it is probably because
            // it is a generated class that hasn't been loaded yet
            return;
        }
        try
        {
            TypeReference[] parameters = methodReference.parameters();
            if ( methodReference.isConstructor() )
            {
                clazz.getDeclaredConstructor(
                        stream( parameters ).map( ByteCodeUtils::asClass ).toArray( Class<?>[]::new ) );
            }
            else
            {
                Method method = clazz.getMethod( methodReference.name(), stream( parameters )
                        .map( ByteCodeUtils::asClass ).toArray( Class<?>[]::new ) );
                TypeReference returnType = typeReference( method.getReturnType() );
                if ( !methodReference.returns().name().equals( returnType.name() ) )
                {
                    throw new AssertionError( format( "Wrong return type of `%s::%s`, expected %s got %s",
                            clazz.getSimpleName(), methodReference.name(), methodReference.returns(), returnType ) );
                }
            }
        }
        catch ( NoSuchMethodException e )
        {
            String[] allMethods = stream( clazz.getMethods() ).map( Method::toString ).toArray( String[]::new );
            String methodName = methodReference.returns().fullName() + " " + methodReference.name() + "(" +
                                join( ", ", stream( methodReference.parameters() ).map( TypeReference::fullName )
                                        .toArray( String[]::new ) ) + ")";
            throw new AssertionError( format( "%s does not exists.%n Class %s has the following methods:%n%s",
                    methodName,
                    clazz.getCanonicalName(),
                    join( format( "%n    " ), allMethods ) ) );
        }
    }

    private static Class<?> asClass( TypeReference typeReference )
    {
        try
        {
            String className = typeReference.baseName();
            switch ( className )
            {
            case "byte":
                return typeReference.isArray() ? byte[].class : byte.class;
            case "char":
                return typeReference.isArray() ? char[].class : char.class;
            case "short":
                return typeReference.isArray() ? short[].class : short.class;
            case "int":
                return typeReference.isArray() ? int[].class : int.class;
            case "long":
                return typeReference.isArray() ? long[].class : long.class;
            case "float":
                return typeReference.isArray() ? float[].class : float.class;
            case "double":
                return typeReference.isArray() ? double[].class : double.class;
            case "boolean":
                return typeReference.isArray() ? boolean[].class : boolean.class;
            default:
                return Class.forName( className( typeReference ) );
            }
        }
        catch ( ClassNotFoundException e )
        {
            throw new AssertionError( format( "%s does not exists", typeReference ) );
        }
    }

}
