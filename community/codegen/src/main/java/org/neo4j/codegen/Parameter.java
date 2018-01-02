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

import static java.util.Objects.requireNonNull;
import static org.neo4j.codegen.TypeReference.VOID;
import static org.neo4j.codegen.TypeReference.typeReference;

public class Parameter
{
    public static Parameter param( Class<?> type, String name )
    {
        return param( typeReference( type ), name );
    }

    public static Parameter param( TypeReference type, String name )
    {
        return new Parameter( requireNonNull( type, "TypeReference" ), requireValidName( name ) );
    }

    static final Parameter[] NO_PARAMETERS = new Parameter[0];

    private final TypeReference type;
    private final String name;

    private Parameter( TypeReference type, String name )
    {
        if ( type == VOID )
        {
            throw new IllegalArgumentException( "Variables cannot be declared as void." );
        }
        this.type = type;
        this.name = name;
    }

    @Override
    public String toString()
    {
        return writeTo( new StringBuilder() ).toString();
    }

    StringBuilder writeTo( StringBuilder result )
    {
        result.append( "Parameter[ " );
        type.writeTo( result );
        return result.append( " " ).append( name ).append( " ]" );
    }

    public TypeReference type()
    {
        return type;
    }

    public String name()
    {
        return name;
    }

    static String requireValidName( String name )
    {
        if ( name == null )
        {
            throw new NullPointerException( "name" );
        }
        notKeyword( name );
        if ( !Character.isJavaIdentifierStart( name.codePointAt( 0 ) ) )
        {
            throw new IllegalArgumentException( "Invalid name: " + name );
        }
        for ( int i = 0, cp; i < name.length(); i += Character.charCount( cp ) )
        {
            if ( !Character.isJavaIdentifierPart( cp = name.codePointAt( i ) ) )
            {
                throw new IllegalArgumentException( "Invalid name: " + name );
            }
        }
        return name;
    }

    private static void notKeyword( String name )
    {
        switch ( name )
        {
        case "abstract":
        case "continue":
        case "for":
        case "new":
        case "switch":
        case "assert":
        case "default":
        case "goto":
        case "package":
        case "synchronized":
        case "boolean":
        case "do":
        case "if":
        case "private":
        case "break":
        case "double":
        case "implements":
        case "protected":
        case "throw":
        case "byte":
        case "else":
        case "import":
        case "public":
        case "throws":
        case "case":
        case "enum":
        case "instanceof":
        case "return":
        case "transient":
        case "catch":
        case "extends":
        case "int":
        case "short":
        case "try":
        case "char":
        case "final":
        case "interface":
        case "static":
        case "void":
        case "class":
        case "finally":
        case "long":
        case "strictfp":
        case "volatile":
        case "const":
        case "float":
        case "native":
        case "super":
        case "while":
            throw new IllegalArgumentException( "'" + name + "' is a java keyword" );
        case "this":
        case "null":
        case "true":
        case "false":
            throw new IllegalArgumentException( "'" + name + "' is a reserved name" );
        }
    }

    boolean isVarArg()
    {
        return false;
    }
}
