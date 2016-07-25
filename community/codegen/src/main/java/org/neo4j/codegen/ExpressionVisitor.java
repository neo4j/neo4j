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

public interface ExpressionVisitor
{
    void invoke( Expression target, MethodReference method, Expression[] arguments );

    void invoke( MethodReference method, Expression[] arguments );

    void load( LocalVariable variable);

    void getField( Expression target, FieldReference field );

    void constant( Object value );

    void getStatic( FieldReference field );

    void loadThis( String sourceName );

    void newInstance( TypeReference type );

    void not( Expression expression );

    void ternary( Expression test, Expression onTrue, Expression onFalse );

    void ternaryOnNull( Expression test, Expression onTrue, Expression onFalse );

    void ternaryOnNonNull( Expression test, Expression onTrue, Expression onFalse );

    void equal( Expression lhs, Expression rhs, TypeReference type );

    void or( Expression lhs, Expression rhs );

    void and( Expression lhs, Expression rhs );

    void addInts( Expression lhs, Expression rhs );

    void addLongs( Expression lhs, Expression rhs );

    void addDoubles( Expression lhs, Expression rhs );

    void gt( Expression lhs, Expression rhs, TypeReference type );

    void subtractInts( Expression lhs, Expression rhs );

    void subtractLongs( Expression lhs, Expression rhs );

    void subtractDoubles( Expression lhs, Expression rhs );

    void multiplyLongs( Expression lhs, Expression rhs );

    void multiplyDoubles( Expression lhs, Expression rhs );

    void cast( TypeReference type, Expression expression );

    void newArray( TypeReference type, Expression... constants );

    void longToDouble( Expression expression );

    void pop( Expression expression );
}
