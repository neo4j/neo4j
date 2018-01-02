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
package org.neo4j.codegen.source;

import java.lang.reflect.Modifier;
import java.util.List;

import org.neo4j.codegen.ClassEmitter;
import org.neo4j.codegen.Expression;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.MethodDeclaration;
import org.neo4j.codegen.MethodEmitter;
import org.neo4j.codegen.Parameter;
import org.neo4j.codegen.TypeReference;

class ClassWriter implements ClassEmitter
{
    private final StringBuilder target;
    final Configuration configuration;

    ClassWriter( StringBuilder target, Configuration configuration )
    {
        this.target = target;
        this.configuration = configuration;
    }

    void declarePackage( TypeReference type )
    {
        append( "package " ).append( type.packageName() ).append( ";\n" );
    }

    void javadoc( String javadoc )
    {
        append( "/** " ).append( javadoc ).append( " */\n" );
    }

    void publicClass( TypeReference type )
    {
        append( "public class " ).append( type.simpleName() );
    }

    void extendClass( TypeReference base )
    {
        append( " extends " ).append( base.name() ).append( "\n" );
    }

    void implement( TypeReference[] interfaces )
    {
        String prefix = "    implements ";
        for ( TypeReference iFace : interfaces )
        {
            append( prefix ).append( iFace.name() );
            prefix = ", ";
        }
        if ( prefix.length() == 2 )
        {
            append( "\n" );
        }
    }

    void begin()
    {
        append( "{\n" );
    }

    @Override
    public MethodEmitter method( MethodDeclaration signature )
    {
        StringBuilder target = new StringBuilder();
        if ( signature.isConstructor() )
        {
            if ( signature.isStatic() )
            {
                target.append( "    static\n    {\n" );
                return new MethodWriter( target, this );
            }
            else
            {
                target.append( "    public " );
                typeParameters( target, signature );
                target.append( signature.declaringClass().simpleName() );
            }
        }
        else
        {
            target.append( "    " );
            if ( signature.isStatic() )
            {
                target.append( "static " );
            }
            target.append( "public " );
            typeParameters( target, signature );
            target.append( signature.returnType().name() ).append( " " ).append( signature.name() );
        }
        target.append( "(" );
        String prefix = " ";
        for ( Parameter parameter : signature.parameters() )
        {
            target.append( prefix ).append( parameter.type().name() ).append( " " ).append( parameter.name() );
            prefix = ", ";
        }
        if ( prefix.length() > 1 )
        {
            target.append( " " );
        }
        target.append( ')' );
        String sep = " throws ";
        for ( TypeReference thrown : signature.throwsList() )
        {
            target.append( sep ).append( thrown.name() );
            sep = ", ";
        }
        target.append( "\n    {\n" );
        return new MethodWriter( target, this );
    }

    private static void typeParameters( StringBuilder target, MethodDeclaration method )
    {
        List<MethodDeclaration.TypeParameter> parameters = method.typeParameters();
        if ( !parameters.isEmpty() )
        {
            target.append( '<' );
            String sep = "";
            for ( MethodDeclaration.TypeParameter parameter : parameters )
            {
                target.append( sep ).append( parameter.name() );
                TypeReference ext = parameter.extendsBound(), sup = parameter.superBound();
                if ( ext != null )
                {
                    target.append( " extends " ).append( ext.name() );
                }
                else if ( sup != null )
                {
                    target.append( " super " ).append( sup.name() );
                }
                sep = ", ";
            }
            target.append( "> " );
        }
    }

    @Override
    public void done()
    {
        append( "}\n" );
    }

    @Override
    public void field( FieldReference field, Expression value )
    {
        String modifiers = Modifier.toString( field.modifiers() );
        append( "    " ).append( modifiers );
        if ( !modifiers.isEmpty() )
        {
            append( " " );
        }
        append( field.type().name() ).append( ' ' ).append( field.name() );
        if ( value != null )
        {
            append( " = " );
            value.accept( new MethodWriter( target, this ) );
        }
        append( ";\n" );
    }

    StringBuilder append( CharSequence chars )
    {
        return target.append( chars );
    }
}
