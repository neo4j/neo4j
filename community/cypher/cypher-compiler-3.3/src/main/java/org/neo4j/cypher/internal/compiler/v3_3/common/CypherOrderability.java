/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.common;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.neo4j.cypher.internal.compiler.v3_3.spi.NodeIdWrapper;
import org.neo4j.cypher.internal.compiler.v3_3.spi.RelationshipIdWrapper;
import org.neo4j.cypher.internal.frontend.v3_2.IncomparableValuesException;
import org.neo4j.cypher.internal.frontend.v3_2.UnorderableValueException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.helpers.MathUtil;

import static java.lang.String.format;

/**
 * Helper class for dealing with orderability in compiled code.
 *
 * <h1>
 * Orderability
 *
 * <a href="https://github.com/opencypher/openCypher/blob/master/cip/1.accepted/CIP2016-06-14-Define-comparability-and-equality-as-well-as-orderability-and-equivalence.adoc">
 *   The Cypher CIP defining orderability
 * </a>
 *
 * <p>
 * Ascending global sort order of disjoint types:
 *
 * <ul>
 *   <li> MAP types
 *    <ul>
 *      <li> Regular map
 *
 *      <li> NODE
 *
 *      <li> RELATIONSHIP
 *    <ul>
 *
 *  <li> LIST OF ANY?
 *
 *  <li> PATH
 *
 *  <li> STRING
 *
 *  <li> BOOLEAN
 *
 *  <li> NUMBER
 *    <ul>
 *      <li> NaN values are treated as the largest numbers in orderability only (i.e. they are put after positive infinity)
 *    </ul>
 *  <li> VOID (i.e. the type of null)
 * </ul>
 *
 * TBD: POINT and GEOMETRY
 */
public class CypherOrderability
{
    /**
     * Do not instantiate this class
     */
    private CypherOrderability()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Compare with Cypher orderability semantics for disjoint types
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public static int compare( Object lhs, Object rhs )
    {
        if ( lhs == rhs )
        {
            return 0;
        }
        // null is greater than any other type
        else if ( lhs == null )
        {
            return 1;
        }
        else if ( rhs == null )
        {
            return -1;
        }

        // Compare the types
        // TODO: Test coverage for the Orderability CIP
        SuperType leftType = SuperType.ofValue( lhs );
        SuperType rightType = SuperType.ofValue( rhs );

        int typeComparison = SuperType.TYPE_ID_COMPARATOR.compare( leftType, rightType );
        if ( typeComparison != 0 )
        {
            // Types are different an decides the order
            return typeComparison;
        }

        return leftType.comparator.compare( lhs, rhs );
    }

    public enum SuperType
    {
        MAP( 0, FALLBACK_COMPARATOR /*TODO*/ ),
        NODE( 1, NODE_COMPARATOR ),
        RELATIONSHIP( 2, RELATIONSHIP_COMPARATOR ),
        LIST( 3, LIST_COMPARATOR ),
        PATH( 4, PATH_COMPARATOR ),
        STRING( 5, STRING_COMPARATOR ),
        BOOLEAN( 6, BOOLEAN_COMPARATOR ),
        NUMBER( 7, NUMBER_COMPARATOR ),
        VOID( 8, VOID_COMPARATOR );

        public final int typeId;
        public final Comparator comparator;

        SuperType( int typeId, Comparator comparator )
        {
            this.typeId = typeId;
            this.comparator = comparator;
        }

        public boolean isSuperTypeOf( Object value )
        {
            return this == ofValue( value );
        }

        public static SuperType ofValue( Object value )
        {
            if ( value instanceof String || value instanceof Character )
            {
                return STRING;
            }
            else if ( value instanceof Number )
            {
                return NUMBER;
            }
            else if ( value instanceof Boolean )
            {
                return BOOLEAN;
            }
            else if ( value instanceof Map<?,?> )
            {
                return MAP;
            }
            else if ( value instanceof List<?> || value.getClass().isArray() )
            {
                return LIST;
            }
            else if ( value instanceof NodeIdWrapper )
            {
                if ( ((NodeIdWrapper) value).id() == -1 )
                {
                    return VOID;
                }
                return NODE;
            }
            else if ( value instanceof RelationshipIdWrapper )
            {
                if ( ((RelationshipIdWrapper) value).id() == -1 )
                {
                    return VOID;
                }
                return RELATIONSHIP;
            }
            // TODO is Path really the class that compiled runtime will be using?
            else if ( value instanceof Path )
            {
                return PATH;
            }
            throw new UnorderableValueException( value.getClass().getSimpleName() );
        }

        public static Comparator<SuperType> TYPE_ID_COMPARATOR = new Comparator<SuperType>()
        {
            @Override
            public int compare( SuperType left, SuperType right )
            {
                return left.typeId - right.typeId;
            }
        };
    }

    // NOTE: nulls are handled at the top of the public compare() method
    // so the type-specific comparators should not check arguments for null

    private static Comparator<Object> FALLBACK_COMPARATOR = new Comparator<Object>()
    {
        @Override
        public int compare( Object lhs, Object rhs )
        {
            if ( lhs.getClass().isAssignableFrom( rhs.getClass() ) &&
                 lhs instanceof Comparable &&
                 rhs instanceof Comparable )
            {
                return ((Comparable) lhs).compareTo( rhs );
            }

            throw new IncomparableValuesException( lhs.getClass().getSimpleName(), rhs.getClass().getSimpleName() );
        }
    };

    private static Comparator<Object> VOID_COMPARATOR = new Comparator<Object>()
    {
        @Override
        public int compare( Object lhs, Object rhs )
        {
            return 0;
        }
    };

    private static Comparator<Number> NUMBER_COMPARATOR = new Comparator<Number>()
    {
        @Override
        public int compare( Number lhs, Number rhs )
        {
            // If floats, compare float values. If integer types, compare long values
            if ( lhs instanceof Double && rhs instanceof Float )
            {
                return ((Double) lhs).compareTo( rhs.doubleValue() );
            }
            else if ( lhs instanceof Float && rhs instanceof Double )
            {
                return -((Double) rhs).compareTo( lhs.doubleValue() );
            }
            else if ( lhs instanceof Float && rhs instanceof Float )
            {
                return ((Float) lhs).compareTo( (Float) rhs );
            }
            else if ( lhs instanceof Double && rhs instanceof Double )
            {
                return ((Double) lhs).compareTo( (Double) rhs );
            }
            // Right hand side is neither Float nor Double
            else if ( lhs instanceof Double || lhs instanceof Float )
            {
                return MathUtil.compareDoubleAgainstLong( lhs.doubleValue(), rhs.longValue() );
            }
            // Left hand side is neither Float nor Double
            else if ( rhs instanceof Double || rhs instanceof Float )
            {
                return -MathUtil.compareDoubleAgainstLong( rhs.doubleValue(), lhs.longValue() );
            }
            // Everything else is a long from Cypher's point-of-view
            return Long.compare( lhs.longValue(), rhs.longValue() );
        }
    };

    private static Comparator<Object> STRING_COMPARATOR = new Comparator<Object>()
    {
        @Override
        public int compare( Object lhs, Object rhs )
        {
            if ( lhs instanceof Character && rhs instanceof String )
            {
                return lhs.toString().compareTo( (String) rhs );
            }
            else if ( lhs instanceof String && rhs instanceof Character )
            {
                return ((String) lhs).compareTo( rhs.toString() );
            }
            else
            {
                return ((Comparable) lhs).compareTo( rhs );
            }
        }
    };

    private static Comparator<Boolean> BOOLEAN_COMPARATOR = new Comparator<Boolean>()
    {
        @Override
        public int compare( Boolean lhs, Boolean rhs )
        {
            return lhs.compareTo( rhs );
        }
    };

    private static Comparator<NodeIdWrapper> NODE_COMPARATOR = new Comparator<NodeIdWrapper>()
    {
        @Override
        public int compare( NodeIdWrapper lhs, NodeIdWrapper rhs )
        {
            return Long.compare( lhs.id(), rhs.id() );
        }
    };

    private static Comparator<RelationshipIdWrapper> RELATIONSHIP_COMPARATOR = new Comparator<RelationshipIdWrapper>()
    {
        @Override
        public int compare( RelationshipIdWrapper lhs, RelationshipIdWrapper rhs )
        {
            return Long.compare( lhs.id(), rhs.id() );
        }
    };

    // TODO test
    private static Comparator<Path> PATH_COMPARATOR = new Comparator<Path>()
    {
        @Override
        public int compare( Path lhs, Path rhs )
        {
            Iterator<PropertyContainer> lhsIter = lhs.iterator();
            Iterator<PropertyContainer> rhsIter = lhs.iterator();
            while ( lhsIter.hasNext() && rhsIter.hasNext() )
            {
                int result = CypherOrderability.compare( lhsIter.next(), rhsIter.next() );
                if ( 0 != result )
                {
                    return result;
                }
            }
            return (lhsIter.hasNext()) ? 1
                                       : (rhsIter.hasNext()) ? -1
                                                             : 0;
        }
    };

    private static Comparator<Object> LIST_COMPARATOR = new Comparator<Object>()
    {
        @Override
        public int compare( Object lhs, Object rhs )
        {
            Iterator lhsIter = toIterator( lhs );
            Iterator rhsIter = toIterator( rhs );
            while ( lhsIter.hasNext() && rhsIter.hasNext() )
            {
                int result = CypherOrderability.compare( lhsIter.next(), rhsIter.next() );
                if ( 0 != result )
                {
                    return result;
                }
            }
            return (lhsIter.hasNext()) ? 1
                                       : (rhsIter.hasNext()) ? -1
                                                             : 0;
        }

        private Iterator toIterator( Object o )
        {
            Class clazz = o.getClass();
            if ( Iterable.class.isAssignableFrom( clazz) )
            {
                return ((Iterable) o).iterator();
            }
            else if ( Object[].class.isAssignableFrom( clazz) )
            {
                return Arrays.stream( (Object[]) o ).iterator();
            }
            else if ( clazz.equals( int[].class ) )
            {
                return IntStream.of( (int[]) o ).iterator();
            }
            else if ( clazz.equals( long[].class ) )
            {
                return LongStream.of( (long[]) o ).iterator();
            }
            else if ( clazz.equals( float[].class ) )
            {
                return IntStream.range( 0, ((float[]) o).length ).mapToObj( i -> ((float[]) o)[i] ).iterator();
            }
            else if ( clazz.equals( double[].class ) )
            {
                return DoubleStream.of( (double[]) o ).iterator();
            }
            else if ( clazz.equals( String[].class ) )
            {
                return Arrays.stream( (String[]) o ).iterator();
            }
            else if ( clazz.equals( boolean[].class ) )
            {
                // TODO Is there a better way to covert boolean[] to Iterator?
                return IntStream.range( 0, ((boolean[]) o).length ).mapToObj( i -> ((boolean[]) o)[i] ).iterator();
            }
            else if ( clazz.equals( Boolean[].class ) )
            {
                return Arrays.stream( (Boolean[]) o ).iterator();
            }
            else
            {
                throw new UnsupportedOperationException( format( "Can not convert to iterator: %s", clazz.getName() ) );
            }
        }
    };
}
