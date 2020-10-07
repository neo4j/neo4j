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
package org.neo4j.cypher.internal.ast.factory;

import java.util.List;

import org.neo4j.cypher.internal.ast.factory.ASTFactory.StringPos;

/**
 * Factory for constructing AST expressions.
 * <p>
 * This interface is generic in many dimensions, in order to support type-safe construction of ASTs
 * without depending on the concrete AST type. This architecture allows code which creates/manipulates AST
 * to live independently of the AST, and thus makes sharing and reuse of these components much easier.
 * <p>
 * The factory contains methods for creating AST representing all of Cypher 9 expressions, as defined
 * at `https://github.com/opencypher/openCypher/`, and implemented in `https://github.com/opencypher/front-end`.
 */
public interface ASTExpressionFactory<
        EXPRESSION,
        PATTERN,
        VARIABLE extends EXPRESSION,
        PROPERTY extends EXPRESSION,
        MAP_PROJECTION_ITEM,
        POS>
{
    VARIABLE newVariable( POS p, String name );

    EXPRESSION newParameter( POS p, VARIABLE v );

    EXPRESSION newParameter( POS p, String offset );

    EXPRESSION oldParameter( POS p, VARIABLE v );

    EXPRESSION newDouble( POS p, String image );

    EXPRESSION newDecimalInteger( POS p, String image, boolean negated );

    EXPRESSION newHexInteger( POS p, String image, boolean negated );

    EXPRESSION newOctalInteger( POS p, String image, boolean negated );

    EXPRESSION newString( POS p, String image );

    EXPRESSION newTrueLiteral( POS p );

    EXPRESSION newFalseLiteral( POS p );

    EXPRESSION newNullLiteral( POS p );

    EXPRESSION listLiteral( POS p, List<EXPRESSION> values );

    EXPRESSION mapLiteral( POS p, List<StringPos<POS>> keys, List<EXPRESSION> values );

    EXPRESSION hasLabelsOrTypes( EXPRESSION subject, List<StringPos<POS>> labels );

    PROPERTY property( EXPRESSION subject, StringPos<POS> propertyKeyName );

    EXPRESSION or( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION xor( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION and( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION ands( List<EXPRESSION> exprs );

    EXPRESSION not( EXPRESSION e );

    EXPRESSION plus( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION minus( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION multiply( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION divide( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION modulo( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION pow( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION unaryPlus( EXPRESSION e );

    EXPRESSION unaryMinus( EXPRESSION e );

    EXPRESSION eq( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION neq( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION neq2( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION lte( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION gte( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION lt( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION gt( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION regeq( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION startsWith( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION endsWith( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION contains( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION in( POS p, EXPRESSION lhs, EXPRESSION rhs );

    EXPRESSION isNull( EXPRESSION e );

    EXPRESSION listLookup( EXPRESSION list, EXPRESSION index );

    EXPRESSION listSlice( POS p, EXPRESSION list, EXPRESSION start, EXPRESSION end );

    EXPRESSION newCountStar( POS p );

    EXPRESSION functionInvocation( POS p, List<String> namespace, String name, boolean distinct, List<EXPRESSION> arguments );

    EXPRESSION listComprehension( POS p, VARIABLE v, EXPRESSION list, EXPRESSION where, EXPRESSION projection );

    EXPRESSION patternComprehension( POS p, VARIABLE v, PATTERN pattern, EXPRESSION where, EXPRESSION projection );

    EXPRESSION filterExpression( POS p, VARIABLE v, EXPRESSION list, EXPRESSION where );

    EXPRESSION extractExpression( POS p, VARIABLE v, EXPRESSION list, EXPRESSION where, EXPRESSION projection );

    EXPRESSION reduceExpression( POS p, VARIABLE acc, EXPRESSION accExpr, VARIABLE v, EXPRESSION list, EXPRESSION innerExpr );

    EXPRESSION allExpression( POS p, VARIABLE v, EXPRESSION list, EXPRESSION where );

    EXPRESSION anyExpression( POS p, VARIABLE v, EXPRESSION list, EXPRESSION where );

    EXPRESSION noneExpression( POS p, VARIABLE v, EXPRESSION list, EXPRESSION where );

    EXPRESSION singleExpression( POS p, VARIABLE v, EXPRESSION list, EXPRESSION where );

    EXPRESSION patternExpression( POS p, PATTERN pattern );

    EXPRESSION existsSubQuery( POS p, List<PATTERN> patterns, EXPRESSION where );

    EXPRESSION mapProjection( POS p, VARIABLE v, List<MAP_PROJECTION_ITEM> items );

    MAP_PROJECTION_ITEM mapProjectionLiteralEntry( StringPos<POS> property, EXPRESSION value );

    MAP_PROJECTION_ITEM mapProjectionProperty( StringPos<POS> property );

    MAP_PROJECTION_ITEM mapProjectionVariable( VARIABLE v );

    MAP_PROJECTION_ITEM mapProjectionAll( POS p );

    EXPRESSION caseExpression( POS p, EXPRESSION e, List<EXPRESSION> whens, List<EXPRESSION> thens, EXPRESSION elze );

    POS inputPosition( int offset, int line, int column );
}
