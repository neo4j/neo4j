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
    public void load( TypeReference type, String name )
    {
        result.append( "load{type=" );
        if ( type == null )
        {
            result.append( "null" );
        }
        else
        {
            type.writeTo( result );
        }
        result.append( ", name=" ).append( name ).append( "}" );
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
    public void eq( Expression lhs, Expression rhs )
    {
        result.append( "eq(" );
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
    public void add( Expression lhs, Expression rhs )
    {
        result.append( "add(" );
        lhs.accept( this );
        result.append( " + " );
        rhs.accept( this );
        result.append( ")" );
    }

    @Override
    public void gt( Expression lhs, Expression rhs )
    {
        result.append( "gt(" );
        lhs.accept( this );
        result.append( " > " );
        rhs.accept( this );
        result.append( ")" );
    }

    @Override
    public void sub( Expression lhs, Expression rhs )
    {
        result.append( "sub(" );
        lhs.accept( this );
        result.append( " - " );
        rhs.accept( this );
        result.append( ")" );
    }
}
