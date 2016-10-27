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
package org.neo4j.codegen.source;

import org.apache.commons.lang3.StringEscapeUtils;

import java.util.Deque;
import java.util.LinkedList;
import java.util.function.Consumer;

import org.neo4j.codegen.Expression;
import org.neo4j.codegen.ExpressionVisitor;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.LocalVariable;
import org.neo4j.codegen.MethodEmitter;
import org.neo4j.codegen.MethodReference;
import org.neo4j.codegen.TypeReference;

class MethodSourceWriter implements MethodEmitter, ExpressionVisitor
{
    private static final Runnable BOTTOM = () -> {
        throw new IllegalStateException( "Popped too many levels!" );
    }, LEVEL = () -> {
    };
    private static final String INDENTATION = "    ";
    private final StringBuilder target;
    private final ClassSourceWriter classSourceWriter;
    private final Deque<Runnable> level = new LinkedList<>();

    public MethodSourceWriter( StringBuilder target, ClassSourceWriter classSourceWriter )
    {
        this.target = target;
        this.classSourceWriter = classSourceWriter;
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
        classSourceWriter.append( target );
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
    public void assignVariableInScope( LocalVariable local, Expression value )
    {
        indent().append( local.name() ).append( " = " );
        value.accept( this );
        append( ";\n" );
    }

    @Override
    public void assign( LocalVariable variable, Expression value )
    {
        indent().append( variable.type().name() ).append( ' ' ).append( variable.name() ).append( " = " );
        value.accept( this );
        append( ";\n" );
    }

    @Override
    public void beginWhile( Expression...tests )
    {
        indent().append( "while( " );
        String sep = "";
        for (Expression test: tests)
        {
            append( sep );
            test.accept( this );
            sep = " && ";
        }
        append( " )\n" );
        indent().append( "{\n" );
        level.push( LEVEL );
    }

    @Override
    public void beginIf( Expression...tests )
    {
        indent().append( "if ( " );
        String sep = "";
        for (Expression test: tests)
        {
            append( sep );
            test.accept( this );
            sep = " && ";
        }
        append( " )\n" );
        indent().append( "{\n" );
        level.push( LEVEL );
    }

    @Override
    public void beginIfNot( Expression...tests )
    {
        Expression[] nots = new Expression[tests.length];
        for ( int i = 0; i < tests.length; i++ )
        {
            nots[i] = Expression.not( tests[i] );
        }
        beginIf( nots );
    }

    @Override
    public void beginIfNull( Expression...tests )
    {
        Expression[] nulls = new Expression[tests.length];
        for ( int i = 0; i < tests.length; i++ )
        {
            nulls[i] = Expression.equal(tests[i], Expression.constant( null ) );
        }
        beginIf(nulls);
    }

    @Override
    public void beginIfNonNull( Expression...tests )
    {
        Expression[] notNulls = new Expression[tests.length];
        for ( int i = 0; i < tests.length; i++ )
        {
            notNulls[i] = Expression.not(Expression.equal(tests[i], Expression.constant( null ) ));
        }
        beginIf(notNulls);
    }

    @Override
    public <T> void tryCatchBlock( Consumer<T> body, Consumer<T> handler, LocalVariable exception, T block)
    {

        indent().append( "try\n" );
        indent().append( "{\n" );
        level.push( LEVEL );
        body.accept( block );
        level.pop();
        indent().append( "}\n" );
        indent().append( "catch ( " )
                .append( exception.type().name() ).append( " " )
                .append( exception.name() )
                .append( " )\n" );
        indent().append( "{\n" );
        level.push( LEVEL );
        handler.accept( block );
        level.pop();
        indent().append( "}\n" );
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
    public void load( LocalVariable variable )
    {
        append( variable.name() );
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
    public void ternaryOnNull( Expression test, Expression onTrue, Expression onFalse )
    {
        ternary( Expression.equal( test, Expression.constant( null ) ),
                onTrue, onFalse );
    }

    @Override
    public void ternaryOnNonNull( Expression test, Expression onTrue, Expression onFalse )
    {
        ternary( Expression.not(
                Expression.equal( test, Expression.constant( null ) )),
                onTrue, onFalse );
    }

    @Override
    public void equal( Expression lhs, Expression rhs )
    {
        binaryOperation( lhs, rhs, " == " );
    }

    @Override
    public void or( Expression lhs, Expression rhs )
    {
        binaryOperation( lhs, rhs, " || " );
    }

    @Override
    public void and( Expression lhs, Expression rhs )
    {
        binaryOperation( lhs, rhs, " && " );

    }

    @Override
    public void addInts( Expression lhs, Expression rhs )
    {
        add( lhs, rhs );
    }

    @Override
    public void addLongs( Expression lhs, Expression rhs )
    {
        add( lhs, rhs );
    }

    @Override
    public void addDoubles( Expression lhs, Expression rhs )
    {
        add( lhs, rhs );
    }

    private void add( Expression lhs, Expression rhs )
    {
        binaryOperation( lhs, rhs, " + " );
    }

    @Override
    public void gt( Expression lhs, Expression rhs )
    {
        binaryOperation( lhs, rhs, " > " );
    }

    @Override
    public void gte( Expression lhs, Expression rhs )
    {
        binaryOperation( lhs, rhs, " >= " );
    }

    @Override
    public void lt( Expression lhs, Expression rhs )
    {
        binaryOperation( lhs, rhs, " < " );
    }

    @Override
    public void lte( Expression lhs, Expression rhs )
    {
        binaryOperation( lhs, rhs, " <= " );
    }

    @Override
    public void subtractInts( Expression lhs, Expression rhs )
    {
        sub( lhs, rhs);
    }

    @Override
    public void subtractLongs( Expression lhs, Expression rhs )
    {
        sub( lhs, rhs);
    }

    @Override
    public void subtractDoubles( Expression lhs, Expression rhs )
    {
        sub( lhs, rhs);
    }

    private void sub( Expression lhs, Expression rhs )
    {
        lhs.accept( this );
        append( " - " );
        rhs.accept( this );
    }

    @Override
    public void multiplyLongs( Expression lhs, Expression rhs )
    {
        mul( lhs, rhs);
    }

    @Override
    public void multiplyDoubles( Expression lhs, Expression rhs )
    {
        mul( lhs, rhs);
    }

    private void mul( Expression lhs, Expression rhs )
    {
        lhs.accept( this );
        append( " * " );
        rhs.accept( this );
    }

    private void div( Expression lhs, Expression rhs )
    {
        lhs.accept( this );
        append( " / " );
        rhs.accept( this );
    }

    @Override
    public void cast( TypeReference type, Expression expression )
    {
        append( "(" );
        append( "(" ).append( type.name() ).append( ") " );
        expression.accept( this );
        append( ")" );
    }

    @Override
    public void newArray( TypeReference type, Expression... constants )
    {
        append( "new " ).append( type.name() ).append( "[]{" );
        String sep = "";
        for ( Expression constant : constants )
        {
            append( sep );
            constant.accept( this );
            sep = ", ";
        }
        append( "}" );
    }

    @Override
    public void longToDouble( Expression expression )
    {
        cast( TypeReference.typeReference( double.class ), expression );
    }

    @Override
    public void pop( Expression expression )
    {
        expression.accept( this );
    }

    private void binaryOperation(Expression lhs, Expression rhs, String operator)
    {
        lhs.accept( this );
        append( operator );
        rhs.accept( this );
    }
}
