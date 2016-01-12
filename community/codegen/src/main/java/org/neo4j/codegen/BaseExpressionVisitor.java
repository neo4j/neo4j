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

/**
 * Default implementation of {@link ExpressionVisitor}
 */
public abstract class BaseExpressionVisitor implements ExpressionVisitor
{
    @Override
    public void invoke( Expression target, MethodReference method, Expression[] arguments )
    {

    }

    @Override
    public void invoke( MethodReference method, Expression[] arguments )
    {

    }

    @Override
    public void load( LocalVariable variable )
    {

    }

    @Override
    public void getField( Expression target, FieldReference field )
    {

    }

    @Override
    public void constant( Object value )
    {

    }

    @Override
    public void getStatic( FieldReference field )
    {

    }

    @Override
    public void loadThis( String sourceName )
    {

    }

    @Override
    public void newInstance( TypeReference type )
    {

    }

    @Override
    public void not( Expression expression )
    {

    }

    @Override
    public void ternary( Expression test, Expression onTrue, Expression onFalse )
    {

    }

    @Override
    public void eq( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void or( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void add( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void gt( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void sub( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void cast( TypeReference type, Expression expression )
    {

    }

    @Override
    public void newArray( TypeReference type, Expression... constants )
    {

    }
}
