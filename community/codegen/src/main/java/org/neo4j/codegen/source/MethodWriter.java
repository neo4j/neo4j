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

import org.apache.commons.lang3.StringEscapeUtils;

import java.util.Deque;
import java.util.LinkedList;

import org.neo4j.codegen.Expression;
import org.neo4j.codegen.ExpressionVisitor;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.LocalVariable;
import org.neo4j.codegen.MethodEmitter;
import org.neo4j.codegen.MethodReference;
import org.neo4j.codegen.Parameter;
import org.neo4j.codegen.Resource;
import org.neo4j.codegen.TypeReference;

class MethodWriter implements MethodEmitter, ExpressionVisitor
{
    private static final Runnable BOTTOM = new Runnable()
    {
        @Override
        public void run()
        {
            throw new IllegalStateException( "Popped too many levels!" );
        }
    }, LEVEL = new Runnable()
    {
        @Override
        public void run()
        {
        }
    };
    private static final String INDENTATION = "    ";
    private final StringBuilder target;
    private final ClassWriter classWriter;
    private final Deque<Runnable> level = new LinkedList<>();

    public MethodWriter( StringBuilder target, ClassWriter classWriter )
    {
        this.target = target;
        this.classWriter = classWriter;
        this.level.push( BOTTOM );
        this.level.push( LEVEL );
    }

    private StringBuilder indent()
    {
        for ( int level = this.level.size(); level-- > 0; )
        {
            target.append( INDENTATION );
        }
        return target;
    }

    private StringBuilder append( CharSequence text )
    {
        return target.append( text );
    }

    @Override
    public void done()
    {
        if ( level.size() != 1 )
        {
            throw new IllegalStateException( "unbalanced blocks!" );
        }
        classWriter.append( target );
    }

    @Override
    public void expression( Expression expression )
    {
        indent();
        expression.accept( this );
        target.append( ";\n" );
    }

    @Override
    public void put( Expression target, FieldReference field, Expression value )
    {
        indent();
        target.accept( this );
        append( "." );
        append( field.name() );
        append( " = " );
        value.accept( this );
        append( ";\n" );
    }

    @Override
    public void returns()
    {
        indent().append( "return;\n" );
    }

    @Override
    public void returns( Expression value )
    {
        indent().append( "return " );
        value.accept( this );
        append( ";\n" );
    }

    @Override
    public void declare( LocalVariable local )
    {
        indent().append( local.type().name() ).append( ' ' ).append( local.name() ).append( ";\n" );
    }

    @Override
    public void assign( LocalVariable local, Expression value )
    {
        indent().append( local.name() ).append( " = " );
        value.accept( this );
        append( ";\n" );
    }

    @Override
    public void beginForEach( Parameter local, Expression iterable )
    {
        indent().append( "for ( " ).append( local.type().name() ).append( " " ).append( local.name() ).append( " : " );
        iterable.accept( this );
        append( " )\n" );
        indent().append( "{\n" );
        level.push( LEVEL );
    }

    @Override
    public void assign( TypeReference type, String name, Expression value )
    {
        indent().append( type.name() ).append( ' ' ).append( name ).append( " = " );
        value.accept( this );
        append( ";\n" );
    }

    @Override
    public void beginWhile( Expression test )
    {
        indent().append( "while( " );
        test.accept( this );
        append( " )\n" );
        indent().append( "{\n" );
        level.push( LEVEL );
    }

    @Override
    public void beginIf( Expression test )
    {
        indent().append( "if ( " );
        test.accept( this );
        append( " )\n" );
        indent().append( "{\n" );
        level.push( LEVEL );
    }

    @Override
    public void beginCatch( Parameter exception )
    {
        indent().append( "catch ( " ).append( exception.type().name() ).append( " " ).append( exception.name() )
                .append( " )\n" );
        indent().append( "{\n" );
        level.push( LEVEL );
    }

    @Override
    public void beginFinally()
    {
        indent().append( "finally\n" );
        indent().append( "{\n" );
        level.push( LEVEL );
    }

    @Override
    public void beginTry( final Resource... resources )
    {
        if ( resources.length > 0 && classWriter.configuration.isSet( SourceCode.SIMPLIFY_TRY_WITH_RESOURCE ) )
        {
            for ( Resource resource : resources )
            {
                indent().append( resource.type().name() ).append( " " ).append( resource.name() ).append( " = " );
                resource.producer().accept( this );
                append( ";\n" );
            }
            indent().append( "try\n" );
            indent().append( "{" );
            level.push( new Runnable()
            {
                @Override
                public void run()
                {
                    beginFinally();
                    for ( Resource resource : resources )
                    {
                        indent().append( resource.name() ).append( ".close();\n" );
                    }
                    endBlock();
                }
            } );
        }
        else
        {
            indent().append( "try" );
            if ( resources.length > 0 )
            {
                String sep = " ( ";
                for ( Resource resource : resources )
                {
                    append( sep ).append( resource.type().name() ).append( " " ).append( resource.name() )
                            .append( " = " );
                    resource.producer().accept( this );
                    sep = "; ";
                }
                append( " )" );
            }
            append( "\n" );
            indent().append( "{\n" );
            level.push( LEVEL );
        }
    }

    @Override
    public void throwException( Expression exception )
    {
        indent().append( "throw " );
        exception.accept( this );
        append( ";\n" );
    }

    @Override
    public void endBlock()
    {
        Runnable action = level.pop();
        indent().append( "}\n" );
        action.run();
    }

    @Override
    public void invoke( Expression target, MethodReference method, Expression[] arguments )
    {
        target.accept( this );
        if ( !method.isConstructor() )
        {
            append( "." ).append( method.name() );
        }
        arglist( arguments );
    }

    @Override
    public void invoke( MethodReference method, Expression[] arguments )
    {
        append( method.owner().name() ).append( '.' ).append( method.name() );
        arglist( arguments );
    }

    private void arglist( Expression[] arguments )
    {
        append( "(" );
        String sep = " ";
        for ( Expression argument : arguments )
        {
            append( sep );
            argument.accept( this );
            sep = ", ";
        }
        if ( sep.length() > 1 )
        {
            append( " " );
        }
        append( ")" );
    }

    @Override
    public void load( TypeReference type, String name )
    {
        append( name );
    }

    @Override
    public void getField( Expression target, FieldReference field )
    {
        target.accept( this );
        append( "." ).append( field.name() );
    }

    @Override
    public void constant( Object value )
    {
        if ( value == null )
        {
            append( "null" );
        }
        else if ( value instanceof String )
        {
            append( "\"" ).append( StringEscapeUtils.escapeJava( (String) value ) ).append( '"' );
        }
        else if ( value instanceof Integer )
        {
            append( value.toString() );
        }
        else if ( value instanceof Long )
        {
            append( value.toString() ).append( 'L' );
        }
        else if ( value instanceof Double )
        {
            append( value.toString() );
        }
        else if ( value instanceof Boolean )
        {
            append( value.toString() );
        }
        else
        {
            throw new UnsupportedOperationException( value.getClass() + " constants" );
        }
    }

    @Override
    public void getStatic( FieldReference field )
    {
        append( field.owner().name() ).append( "." ).append( field.name() );
    }

    @Override
    public void loadThis( String sourceName )
    {
        append( sourceName );
    }

    @Override
    public void newInstance( TypeReference type )
    {
        append( "new " ).append( type.name() );
    }

    @Override
    public void not( Expression expression )
    {
        append( "!( " );
        expression.accept( this );
        append( " )" );
    }

    @Override
    public void ternary( Expression test, Expression onTrue, Expression onFalse )
    {
        append( "((" );
        test.accept( this );
        append( ") ? (" );
        onTrue.accept( this );
        append( ") : (" );
        onFalse.accept( this );
        append( "))" );
    }

    @Override
    public void eq( Expression lhs, Expression rhs )
    {
        lhs.accept( this );
        append( " == " );
        rhs.accept( this );
    }

    @Override
    public void or( Expression lhs, Expression rhs )
    {
        lhs.accept( this );
        append( " || " );
        rhs.accept( this );
    }

    @Override
    public void add( Expression lhs, Expression rhs )
    {
        lhs.accept( this );
        append( " + " );
        rhs.accept( this );
    }

    @Override
    public void gt( Expression lhs, Expression rhs )
    {
        lhs.accept( this );
        append( " > " );
        rhs.accept( this );
    }

    @Override
    public void sub( Expression lhs, Expression rhs )
    {
        lhs.accept( this );
        append( " - " );
        rhs.accept( this );
    }
}
