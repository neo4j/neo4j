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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.neo4j.cypher.internal.ast.factory.ASTFactory.NULL;

/**
 * Interprets literal AST nodes and output a corresponding java object.
 */
public class LiteralInterpreter implements ASTFactory<NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,Object,Object,Object,NULL,NULL>
{

    public static final String LONG_MIN_VALUE_DECIMAL_STRING = Long.toString( Long.MIN_VALUE ).substring( 1 );
    public static final String LONG_MIN_VALUE_HEXADECIMAL_STRING = "0x" + Long.toString( Long.MIN_VALUE, 16 ).substring( 1 );
    public static final String LONG_MIN_VALUE_OCTAL_STRING_OLD_SYNTAX = "0" + Long.toString( Long.MIN_VALUE, 8 ).substring( 1 );
    public static final String LONG_MIN_VALUE_OCTAL_STRING = "0o" + Long.toString( Long.MIN_VALUE, 8 ).substring( 1 );

    @Override
    public NULL newSingleQuery( List<NULL> nulls )
    {
        throw new UnsupportedOperationException( "newSingleQuery is not a literal" );
    }

    @Override
    public NULL newUnion( NULL p, NULL lhs, NULL rhs, boolean all )
    {
        throw new UnsupportedOperationException( "newUnion is not a literal" );
    }

    @Override
    public NULL periodicCommitQuery( NULL p, String batchSize, NULL loadCsv, List<NULL> aNull )
    {
        throw new UnsupportedOperationException( "periodicCommitQuery is not a literal" );
    }

    @Override
    public NULL fromClause( NULL p, Object e )
    {
        throw new UnsupportedOperationException( "fromClause is not a literal" );
    }

    @Override
    public NULL useClause( NULL p, Object e )
    {
        throw new UnsupportedOperationException( "useClause is not a literal" );
    }

    @Override
    public NULL newReturnClause( NULL p, boolean distinct, boolean returnAll, List<NULL> nulls, List<NULL> order, Object skip, Object limit )
    {
        throw new UnsupportedOperationException( "newReturnClause is not a literal" );
    }

    @Override
    public NULL newReturnGraphClause( NULL p )
    {
        throw new UnsupportedOperationException( "newReturnGraphClause is not a literal" );
    }

    @Override
    public NULL newReturnItem( NULL p, Object e, Object v )
    {
        throw new UnsupportedOperationException( "newReturnItem is not a literal" );
    }

    @Override
    public NULL newReturnItem( NULL p, Object e, int eStartOffset, int eEndOffset )
    {
        throw new UnsupportedOperationException( "newReturnItem is not a literal" );
    }

    @Override
    public NULL orderDesc( Object e )
    {
        throw new UnsupportedOperationException( "orderDesc is not a literal" );
    }

    @Override
    public NULL orderAsc( Object e )
    {
        throw new UnsupportedOperationException( "orderAsc is not a literal" );
    }

    @Override
    public NULL withClause( NULL p, NULL aNull, Object where )
    {
        throw new UnsupportedOperationException( "withClause is not a literal" );
    }

    @Override
    public NULL matchClause( NULL p, boolean optional, List<NULL> nulls, List<NULL> nulls2, Object where )
    {
        throw new UnsupportedOperationException( "matchClause is not a literal" );
    }

    @Override
    public NULL usingIndexHint( NULL p, Object v, String label, List<String> properties, boolean seekOnly )
    {
        throw new UnsupportedOperationException( "usingIndexHint is not a literal" );
    }

    @Override
    public NULL usingJoin( NULL p, List<Object> joinVariables )
    {
        throw new UnsupportedOperationException( "usingJoin is not a literal" );
    }

    @Override
    public NULL usingScan( NULL p, Object v, String label )
    {
        throw new UnsupportedOperationException( "usingScan is not a literal" );
    }

    @Override
    public NULL createClause( NULL p, List<NULL> nulls )
    {
        throw new UnsupportedOperationException( "createClause is not a literal" );
    }

    @Override
    public NULL setClause( NULL p, List<NULL> nulls )
    {
        throw new UnsupportedOperationException( "setClause is not a literal" );
    }

    @Override
    public NULL setProperty( Object o, Object value )
    {
        throw new UnsupportedOperationException( "setProperty is not a literal" );
    }

    @Override
    public NULL setVariable( Object o, Object value )
    {
        throw new UnsupportedOperationException( "setVariable is not a literal" );
    }

    @Override
    public NULL addAndSetVariable( Object o, Object value )
    {
        throw new UnsupportedOperationException( "addAndSetVariable is not a literal" );
    }

    @Override
    public NULL setLabels( Object o, List<StringPos<NULL>> value )
    {
        throw new UnsupportedOperationException( "setLabels is not a literal" );
    }

    @Override
    public NULL removeClause( NULL p, List<NULL> nulls )
    {
        throw new UnsupportedOperationException( "removeClause is not a literal" );
    }

    @Override
    public NULL removeProperty( Object o )
    {
        throw new UnsupportedOperationException( "removeProperty is not a literal" );
    }

    @Override
    public NULL removeLabels( Object o, List<StringPos<NULL>> labels )
    {
        throw new UnsupportedOperationException( "removeLabels is not a literal" );
    }

    @Override
    public NULL deleteClause( NULL p, boolean detach, List<Object> objects )
    {
        throw new UnsupportedOperationException( "deleteClause is not a literal" );
    }

    @Override
    public NULL unwindClause( NULL p, Object e, Object v )
    {
        throw new UnsupportedOperationException( "unwindClause is not a literal" );
    }

    @Override
    public NULL mergeClause( NULL p, NULL aNull, List<NULL> setClauses, List<MergeActionType> actionTypes )
    {
        throw new UnsupportedOperationException( "mergeClause is not a literal" );
    }

    @Override
    public NULL callClause( NULL p, List<String> namespace, String name, List<Object> arguments, List<NULL> nulls, Object where )
    {
        throw new UnsupportedOperationException( "callClause is not a literal" );
    }

    @Override
    public NULL callResultItem( NULL p, String name, Object v )
    {
        throw new UnsupportedOperationException( "callResultItem is not a literal" );
    }

    @Override
    public NULL namedPattern( Object v, NULL aNull )
    {
        throw new UnsupportedOperationException( "namedPattern is not a literal" );
    }

    @Override
    public NULL shortestPathPattern( NULL p, NULL aNull )
    {
        throw new UnsupportedOperationException( "shortestPathPattern is not a literal" );
    }

    @Override
    public NULL allShortestPathsPattern( NULL p, NULL aNull )
    {
        throw new UnsupportedOperationException( "allShortestPathsPattern is not a literal" );
    }

    @Override
    public NULL everyPathPattern( List<NULL> nodes, List<NULL> relationships )
    {
        throw new UnsupportedOperationException( "everyPathPattern is not a literal" );
    }

    @Override
    public NULL nodePattern( NULL p, Object v, List<StringPos<NULL>> labels, Object properties )
    {
        throw new UnsupportedOperationException( "nodePattern is not a literal" );
    }

    @Override
    public NULL relationshipPattern( NULL p, boolean left, boolean right, Object v, List<StringPos<NULL>> relTypes, NULL aNull, Object properties,
                                     boolean legacyTypeSeparator )
    {
        throw new UnsupportedOperationException( "relationshipPattern is not a literal" );
    }

    @Override
    public NULL pathLength( NULL p, String minLength, String maxLength )
    {
        throw new UnsupportedOperationException( "pathLength is not a literal" );
    }

    @Override
    public NULL loadCsvClause( NULL p, boolean headers, Object source, Object v, String fieldTerminator )
    {
        throw new UnsupportedOperationException( "loadCsvClause is not a literal" );
    }

    @Override
    public NULL foreachClause( NULL p, Object v, Object list, List<NULL> nulls )
    {
        throw new UnsupportedOperationException( "foreachClause is not a literal" );
    }

    @Override
    public NULL subqueryClause( NULL p, NULL subquery )
    {
        throw new UnsupportedOperationException( "subqueryClause is not a literal" );
    }

    @Override
    public Object newVariable( NULL p, String name )
    {
        throw new UnsupportedOperationException( "newVariable is not a literal" );
    }

    @Override
    public Object newParameter( NULL p, Object v )
    {
        throw new UnsupportedOperationException( "newParameter is not a literal" );
    }

    @Override
    public Object newParameter( NULL p, String offset )
    {
        throw new UnsupportedOperationException( "newParameter is not a literal" );
    }

    @Override
    public Object oldParameter( NULL p, Object v )
    {
        throw new UnsupportedOperationException( "oldParameter is not a literal" );
    }

    @Override
    public Object newDouble( NULL p, String image )
    {
        return Double.valueOf( image );
    }

    @Override
    public Object newDecimalInteger( NULL p, String image, boolean negated )
    {
        try
        {
            long x = Long.parseLong( image );
            return negated ? -x : x;
        }
        catch ( NumberFormatException e )
        {
            if ( negated && LONG_MIN_VALUE_DECIMAL_STRING.equals( image ) )
            {
                return Long.MIN_VALUE;
            }
            else
            {
                throw e;
            }
        }
    }

    @Override
    public Object newHexInteger( NULL p, String image, boolean negated )
    {
        try
        {
            long x = Long.parseLong( image.substring( 2 ), 16 );
            return negated ? -x : x;
        }
        catch ( NumberFormatException e )
        {
            if ( negated && LONG_MIN_VALUE_HEXADECIMAL_STRING.equals( image ) )
            {
                return Long.MIN_VALUE;
            }
            else
            {
                throw e;
            }
        }
    }

    @Override
    public Object newOctalInteger( NULL p, String image, boolean negated )
    {
        try
        {
            long x;
            if ( image.charAt( 1 ) == 'o' )
            {
                x = Long.parseLong( image.substring( 2 ), 8 );
            }
            else
            {
                x = Long.parseLong( image, 8 );
            }
            return negated ? -x : x;
        }
        catch ( NumberFormatException e )
        {
            if ( negated && ( LONG_MIN_VALUE_OCTAL_STRING.equals( image ) || LONG_MIN_VALUE_OCTAL_STRING_OLD_SYNTAX.equals( image ) ) )
            {
                return Long.MIN_VALUE;
            }
            else
            {
                throw e;
            }
        }
    }

    @Override
    public Object newString( NULL p, String image )
    {
        return image;
    }

    @Override
    public Object newTrueLiteral( NULL p )
    {
        return Boolean.TRUE;
    }

    @Override
    public Object newFalseLiteral( NULL p )
    {
        return Boolean.FALSE;
    }

    @Override
    public Object newNullLiteral( NULL p )
    {
        return null;
    }

    @Override
    public Object listLiteral( NULL p, List<Object> values )
    {
        return values;
    }

    @Override
    public Object mapLiteral( NULL p, List<StringPos<NULL>> keys, List<Object> values )
    {
        HashMap<String,Object> x = new HashMap<>();
        Iterator<StringPos<NULL>> keyIterator = keys.iterator();
        Iterator<Object> valueIterator = values.iterator();
        while ( keyIterator.hasNext() )
        {
            x.put( keyIterator.next().string, valueIterator.next() );
        }
        return x;
    }

    @Override
    public Object hasLabels( Object subject, List<StringPos<NULL>> labels )
    {
        throw new UnsupportedOperationException( "hasLabels is not a literal" );
    }

    @Override
    public Object property( Object subject, StringPos<NULL> propertyKeyName )
    {
        throw new UnsupportedOperationException( "property is not a literal" );
    }

    @Override
    public Object or( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "or is not a literal" );
    }

    @Override
    public Object xor( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "xor is not a literal" );
    }

    @Override
    public Object and( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "and is not a literal" );
    }

    @Override
    public Object ands( List<Object> exprs )
    {
        throw new UnsupportedOperationException( "ands is not a literal" );
    }

    @Override
    public Object not( Object e )
    {
        throw new UnsupportedOperationException( "not is not a literal" );
    }

    @Override
    public Object plus( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "plus is not a literal" );
    }

    @Override
    public Object minus( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "minus is not a literal" );
    }

    @Override
    public Object multiply( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "multiply is not a literal" );
    }

    @Override
    public Object divide( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "divide is not a literal" );
    }

    @Override
    public Object modulo( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "modulo is not a literal" );
    }

    @Override
    public Object pow( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "pow is not a literal" );
    }

    @Override
    public Object unaryPlus( Object e )
    {
        throw new UnsupportedOperationException( "unaryPlus is not a literal" );
    }

    @Override
    public Object unaryMinus( Object e )
    {
        throw new UnsupportedOperationException( "unaryMinus is not a literal" );
    }

    @Override
    public Object eq( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "eq is not a literal" );
    }

    @Override
    public Object neq( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "neq is not a literal" );
    }

    @Override
    public Object neq2( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "neq2 is not a literal" );
    }

    @Override
    public Object lte( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "lte is not a literal" );
    }

    @Override
    public Object gte( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "gte is not a literal" );
    }

    @Override
    public Object lt( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "lt is not a literal" );
    }

    @Override
    public Object gt( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "gt is not a literal" );
    }

    @Override
    public Object regeq( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "regeq is not a literal" );
    }

    @Override
    public Object startsWith( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "startsWith is not a literal" );
    }

    @Override
    public Object endsWith( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "endsWith is not a literal" );
    }

    @Override
    public Object contains( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "contains is not a literal" );
    }

    @Override
    public Object in( NULL p, Object lhs, Object rhs )
    {
        throw new UnsupportedOperationException( "in is not a literal" );
    }

    @Override
    public Object isNull( Object e )
    {
        throw new UnsupportedOperationException( "isNull is not a literal" );
    }

    @Override
    public Object listLookup( Object list, Object index )
    {
        throw new UnsupportedOperationException( "listLookup is not a literal" );
    }

    @Override
    public Object listSlice( NULL p, Object list, Object start, Object end )
    {
        throw new UnsupportedOperationException( "listSlice is not a literal" );
    }

    @Override
    public Object newCountStar( NULL p )
    {
        throw new UnsupportedOperationException( "newCountStar is not a literal" );
    }

    @Override
    public Object functionInvocation( NULL p, List<String> namespace, String name, boolean distinct, List<Object> arguments )
    {
        throw new UnsupportedOperationException( "functionInvocation is not a literal" );
    }

    @Override
    public Object listComprehension( NULL p, Object v, Object list, Object where, Object projection )
    {
        throw new UnsupportedOperationException( "listComprehension is not a literal" );
    }

    @Override
    public Object patternComprehension( NULL p, Object v, NULL aNull, Object where, Object projection )
    {
        throw new UnsupportedOperationException( "patternComprehension is not a literal" );
    }

    @Override
    public Object filterExpression( NULL p, Object v, Object list, Object where )
    {
        throw new UnsupportedOperationException( "filterExpression is not a literal" );
    }

    @Override
    public Object extractExpression( NULL p, Object v, Object list, Object where, Object projection )
    {
        throw new UnsupportedOperationException( "extractExpression is not a literal" );
    }

    @Override
    public Object reduceExpression( NULL p, Object acc, Object accExpr, Object v, Object list, Object innerExpr )
    {
        throw new UnsupportedOperationException( "reduceExpression is not a literal" );
    }

    @Override
    public Object allExpression( NULL p, Object v, Object list, Object where )
    {
        throw new UnsupportedOperationException( "allExpression is not a literal" );
    }

    @Override
    public Object anyExpression( NULL p, Object v, Object list, Object where )
    {
        throw new UnsupportedOperationException( "anyExpression is not a literal" );
    }

    @Override
    public Object noneExpression( NULL p, Object v, Object list, Object where )
    {
        throw new UnsupportedOperationException( "noneExpression is not a literal" );
    }

    @Override
    public Object singleExpression( NULL p, Object v, Object list, Object where )
    {
        throw new UnsupportedOperationException( "singleExpression is not a literal" );
    }

    @Override
    public Object patternExpression( NULL p, NULL aNull )
    {
        throw new UnsupportedOperationException( "patternExpression is not a literal" );
    }

    @Override
    public Object existsSubQuery( NULL p, List<NULL> nulls, Object where )
    {
        throw new UnsupportedOperationException( "existsSubQuery is not a literal" );
    }

    @Override
    public Object mapProjection( NULL p, Object v, List<NULL> nulls )
    {
        throw new UnsupportedOperationException( "mapProjection is not a literal" );
    }

    @Override
    public NULL mapProjectionLiteralEntry( StringPos<NULL> property, Object value )
    {
        throw new UnsupportedOperationException( "mapProjectionLiteralEntry is not a literal" );
    }

    @Override
    public NULL mapProjectionProperty( StringPos<NULL> property )
    {
        throw new UnsupportedOperationException( "mapProjectionProperty is not a literal" );
    }

    @Override
    public NULL mapProjectionVariable( Object v )
    {
        throw new UnsupportedOperationException( "mapProjectionVariable is not a literal" );
    }

    @Override
    public NULL mapProjectionAll( NULL p )
    {
        throw new UnsupportedOperationException( "mapProjectionAll is not a literal" );
    }

    @Override
    public Object caseExpression( NULL p, Object e, List<Object> whens, List<Object> thens, Object elze )
    {
        throw new UnsupportedOperationException( "caseExpression is not a literal" );
    }

    @Override
    public NULL inputPosition( int offset, int line, int column )
    {
        return null;
    }
}
