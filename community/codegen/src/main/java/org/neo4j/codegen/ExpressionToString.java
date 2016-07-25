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

class ExpressionToString implements ExpressionVisitor
{
    private final StringBuilder result;

    public ExpressionToString( StringBuilder result )
    {
        this.result = result;
    }

    @Override
    public void invoke( Expression target, MethodReference method, Expression[] arguments )
    {
        result.append( "invoke{target=" );
        target.accept( this );
        result.append( ", method=" );
        method.writeTo( result );
        result.append( "}(" );
        String sep = "";
        for ( Expression argument : arguments )
        {
            result.append( sep );
            argument.accept( this );
            sep = ", ";
        }
        result.append( ")" );
    }

    @Override
    public void invoke( MethodReference method, Expression[] arguments )
    {
        result.append( "invoke{method=" );
        method.writeTo( result );
        result.append( "}(" );
        String sep = "";
        for ( Expression argument : arguments )
        {
            result.append( sep );
            argument.accept( this );
            sep = ", ";
        }
        result.append( ")" );
    }

    @Override
    public void load(  LocalVariable variable)
    {
        result.append( "load{type=" );
        if ( variable.type() == null )
        {
            result.append( "null" );
        }
        else
        {
            variable.type().writeTo( result );
        }
        result.append( ", name=" ).append( variable.name() ).append( "}" );
    }

    @Override
    public void getField( Expression target, FieldReference field )
    {
        result.append( "get{target=" );
        target.accept( this );
        result.append( ", field=" ).append( field.name() ).append( "}" );
    }

    @Override
    public void constant( Object value )
    {
        result.append( "constant(" ).append( value ).append( ")" );
    }

    @Override
    public void getStatic( FieldReference field )
    {
        result.append( "get{class=" ).append( field.owner() );
        result.append( ", field=" ).append( field.name() ).append( "}" );
    }

    @Override
    public void loadThis( String sourceName )
    {
        result.append( "load{" ).append( sourceName ).append( "}" );
    }

    @Override
    public void newInstance( TypeReference type )
    {
        result.append( "new{type=" );
        type.writeTo( result );
        result.append( "}" );
    }

    @Override
    public void not( Expression expression )
    {
        result.append( "not(" );
        expression.accept( this );
        result.append( ")" );
    }

    @Override
    public void ternary( Expression test, Expression onTrue, Expression onFalse )
    {
        result.append( "ternary{test=" );
        test.accept( this );
        result.append(", onTrue=");
        onTrue.accept( this );
        result.append(", onFalse=");
        onFalse.accept( this );
        result.append( "}" );
    }

    @Override
    public void ternaryOnNull( Expression test, Expression onTrue, Expression onFalse )
    {
        ternary( Expression.equal( test, Expression.constant( null ), TypeReference.OBJECT ),
                onTrue, onFalse );
    }

    @Override
    public void ternaryOnNonNull( Expression test, Expression onTrue, Expression onFalse )
    {
        ternary( Expression.not(
                Expression.equal( test, Expression.constant( null ), TypeReference.OBJECT )),
                onTrue, onFalse );
    }

    @Override
    public void equal( Expression lhs, Expression rhs, TypeReference ignored )
    {
        result.append( "equal(" );
        lhs.accept( this );
        result.append( ", " );
        rhs.accept( this );
        result.append( ")" );
    }

    @Override
    public void or( Expression lhs, Expression rhs )
    {
        result.append( "or(" );
        lhs.accept( this );
        result.append( ", " );
        rhs.accept( this );
        result.append( ")" );
    }

    @Override
    public void and( Expression lhs, Expression rhs )
    {
        result.append( "and(" );
        lhs.accept( this );
        result.append( ", " );
        rhs.accept( this );
        result.append( ")" );
    }

    @Override
    public void addInts( Expression lhs, Expression rhs )
    {
        add(lhs, rhs);
    }

    @Override
    public void addLongs( Expression lhs, Expression rhs )
    {
        add(lhs, rhs);
    }

    @Override
    public void addDoubles( Expression lhs, Expression rhs )
    {
        add(lhs, rhs);
    }

    private void add( Expression lhs, Expression rhs )
    {
        result.append( "add(" );
        lhs.accept( this );
        result.append( " + " );
        rhs.accept( this );
        result.append( ")" );
    }

    @Override
    public void gt( Expression lhs, Expression rhs, TypeReference ignored )
    {
        result.append( "gt(" );
        lhs.accept( this );
        result.append( " > " );
        rhs.accept( this );
        result.append( ")" );
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
        result.append( "sub(" );
        lhs.accept( this );
        result.append( " - " );
        rhs.accept( this );
        result.append( ")" );
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
        result.append( "mul(" );
        lhs.accept( this );
        result.append( " * " );
        rhs.accept( this );
        result.append( ")" );
    }

    private void div( Expression lhs, Expression rhs )
    {
        result.append( "div(" );
        lhs.accept( this );
        result.append( " / " );
        rhs.accept( this );
        result.append( ")" );
    }

    @Override
    public void cast( TypeReference type, Expression expression )
    {
        result.append( "cast{type=" );
        type.writeTo( result );
        result.append( ", expression=" );
        expression.accept( this );
        result.append( "}" );
    }

    @Override
    public void newArray( TypeReference type, Expression... constants )
    {
        result.append( "newArray{type=" );
        type.writeTo( result );
        result.append( ", constants=" );
        String sep = "";
        for ( Expression constant : constants )
        {
            result.append( sep );
            constant.accept( this );
            sep = ", ";
        }
        result.append( "}" );
    }

    @Override
    public void longToDouble( Expression expression )
    {
        result.append( "(double)" );
        expression.accept( this );
    }

    @Override
    public void pop( Expression expression )
    {
        result.append( "pop(" );
        expression.accept( this );
        result.append( ")" );
    }
}
