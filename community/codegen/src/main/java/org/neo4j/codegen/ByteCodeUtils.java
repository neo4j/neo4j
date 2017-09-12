/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.List;

public final class ByteCodeUtils
{
    private ByteCodeUtils()
    {
        throw new UnsupportedOperationException();
    }

    public static String byteCodeName( TypeReference reference )
    {
        StringBuilder builder = new StringBuilder();
        if ( !reference.packageName().isEmpty() )
        {
            builder.append( reference.packageName().replaceAll( "\\.", "/" ) ).append( '/' );
        }
        if ( reference.isInnerClass() )
        {
            builder.append( reference.declaringClassName() ).append( '$' );
        }
        builder.append( reference.name() );
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
        StringBuilder builder = new StringBuilder(  );
        builder.append( "(" );
        for ( TypeReference parameter : reference.parameters() )
        {
            internalType( builder, parameter, false );
        }
        builder.append( ")" );
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
            builder.append( "<" );
            for ( MethodDeclaration.TypeParameter typeParameter : typeParameters )
            {
                builder.append( typeParameter.name() ).append( ":" );
                internalType( builder, typeParameter.extendsBound(), true );
            }
            builder.append( ">" );
        }
        builder.append( "(" );
        for ( Parameter parameter : declaration.parameters() )
        {
            internalType( builder, parameter.type(), showErasure );
        }
        builder.append( ")" );
        internalType( builder, declaration.returnType(), showErasure );
        List<TypeReference> throwsList = declaration.throwsList();
        if ( showErasure && throwsList.stream().anyMatch( TypeReference::isTypeParameter ) )
        {
            builder.append( "^" );
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
        if ( reference.isArray() )
        {
            builder.append( "[" );
        }

        switch ( name )
        {
        case "int":
            builder.append( "I" );
            break;
        case "long":
            builder.append( "J" );
            break;
        case "byte":
            builder.append( "B" );
            break;
        case "short":
            builder.append( "S" );
            break;
        case "char":
            builder.append( "C" );
            break;
        case "float":
            builder.append( "F" );
            break;
        case "double":
            builder.append( "D" );
            break;
        case "boolean":
            builder.append( "Z" );
            break;
        case "void":
            builder.append( "V" );
            break;

        default:
            if ( reference.isTypeParameter() )
            {
                builder.append( "T" ).append( name );
            }
            else
            {
                builder.append( "L" );
                String packageName = reference.packageName().replaceAll( "\\.", "\\/" );
                if ( !packageName.isEmpty() )
                {
                    builder.append( packageName ).append( "/" );
                }
                if ( reference.isInnerClass() )
                {
                    builder.append( reference.declaringClassName() ).append( '$' );
                }
                builder.append( name.replaceAll( "\\.", "\\/" ) );
            }

            List<TypeReference> parameters = reference.parameters();
            if ( showErasure && !parameters.isEmpty() )
            {
                builder.append( "<" );
                parameters
                        .stream()
                        .forEach( p -> internalType( builder, p, true ) );
                builder.append( ">" );
            }
            builder.append( ";" );

        }
        return builder;
    }
}
