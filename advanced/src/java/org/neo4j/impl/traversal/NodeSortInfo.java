/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.traversal;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;

import java.io.Serializable;
import java.util.Comparator;

/**
 * A <CODE>NodeSortInfo</CODE> class represents a sort operation on a set of
 * nodes. The client creates a <CODE>NodeSortInfo</CODE> object that can be
 * passed to {@link java.util.Collections#sort} with a collection of nodes.
 * <p>
 * The <CODE>NodeSortInfo</CODE> class is an imlementation of the typsesafe
 * extensible enum [see Bloch01].
 * <p>
 * <CODE>NodeSortInfo</CODE> contains an inner class <CODE>PropertySortInfo</CODE>
 * that can be used by a client to sort nodes depending on a node property. Here
 * is a sample code snippet sorting nodes from a traverser:
 * <p>
 * <CODE><PRE> // create the traverser ... Traverser traverser =
 * TraverserFactory.getFactory().createTraverser( .... ); // sort on property
 * "title" Traverser sortedTraverser = traverser.sort( new
 * NodeSortInfo.PropertySortInto( PropertyIndex.index( "title" ) ) ); // get the
 * nodes sorted by property "title" while ( sortedTraverser.hasNext() ) { Node
 * node = sortedTraverser.nextNode(); // ... }
 * 
 * </PRE></CODE>
 * <p>
 * @see InternalTraverser
 * @see java.util.Comparator
 * @see java.util.Collections
 */
public abstract class NodeSortInfo<T extends Node> implements Comparator<T>,
    Serializable
{
    /**
     * Compares its two arguments for order. Returns a negative integer, zero,
     * or a positive integer as the first argument is less than, equal to, or
     * greater than the second. See {@link java.util.Comparator} for more
     * information.
     * <p>
     * <CODE>NodeSortInfo</CODE> is intended to be used on collections of
     * nodes. A collection containing a non <CODE>Node</CODE> element should
     * result in a <CODE>ClassCastException</CODE>.
     * 
     * @param node1
     *            first argument
     * @param node2
     *            second argument
     * @throws ClassCastException
     *             if argument type is non <CODE>Node</CODE>
     */
    public abstract int compare( Node node1, Node node2 );

    /**
     * Inner class that represents a sort operation using a node property.
     */
    public static class PropertySortInfo<T extends Node> extends
        NodeSortInfo<T>
    {
        private String key = null;
        private int direction = 1;

        /**
         * Creates a <CODE>PropertySortInfo</CODE> object using property
         * <CODE>index</CODE> for sorting.
         * 
         * @param index
         *            the node property used when sorting
         */
        public PropertySortInfo( String key )
        {
            this.key = key;
        }

        /**
         * Creates a <CODE>PropertySortInfo</CODE> object using property
         * <CODE>index</CODE> for sorting. If <CODE>descending</CODE> is set
         * to true the order will be reversed.
         * 
         * @param index
         *            the node property used when sorting
         * @param descending
         *            if true order will be reversed
         */
        public PropertySortInfo( String key, boolean descending )
        {
            this.key = key;
            if ( descending )
            {
                direction = -1;
            }
        }

        /**
         * Compares its two arguments, see {@link Comparator#compare} for more
         * information.
         * <p>
         * Both arguments has to be of type <CODE>Node</CODE> or a <CODE>ClassCastException</CODE>
         * will be thrown. If the first argument lack the property used for
         * sorting a negative integer will be returned. If the second argument
         * lacks the property a positive integer will be returned. If both
         * arguments lack the property zero will be returned.
         * <p>
         * If the properties are of the same copmarable type (such as Integer or
         * String) the <CODE>compareTo</CODE> method on that property will be
         * invoked passing the second property. If the properties not the same
         * type (but have the compareTo method) they will be converted to
         * strings (via toString method) and compared. If the first property
         * isn't comparable but the second property is a negative integer will
         * be returned. If both properties aren't comparable zero will be
         * returned.
         * 
         * @param obj1
         *            the first argument
         * @param obj2
         *            the second argument
         * @throws ClassCastException
         *             if argument type is non Node
         */
        public int compare( Node node1, Node node2 )
        {
            Object property1 = null;
            Object property2 = null;
            try
            {
                property1 = node1.getProperty( key );
            }
            catch ( NotFoundException e )
            {
                // ok, null then
            }
            try
            {
                property2 = node2.getProperty( key );
            }
            catch ( NotFoundException e )
            {
                // ok, null then
            }

            // check if one or both is null
            if ( property1 == null )
            {
                return (property2 == null ? 0 : -1) * direction;
            }
            if ( property2 == null )
            {
                return 1 * direction;
            }

            // if property1 Integer or String, compare to property2
            if ( property1 instanceof Integer )
            {
                if ( property2 instanceof Integer )
                {
                    return ((Integer) property1)
                        .compareTo( (Integer) property2 )
                        * direction;
                }
                else if ( property2 instanceof String )
                {
                    // alpha numeric
                    String stringValue = property1.toString();
                    return stringValue.compareTo( (String) property2 )
                        * direction;
                }
                else
                {
                    // property1 wins
                    return 1 * direction;
                }
            }
            if ( property1 instanceof String )
            {
                if ( property2 instanceof String )
                {
                    return ((String) property1).compareTo( (String) property2 )
                        * direction;
                }
                else if ( property2 instanceof Integer )
                {
                    // alpha numeric
                    return ((String) property1)
                        .compareTo( property2.toString() )
                        * direction;
                }
                else
                {
                    // property1 wins
                    return 1 * direction;
                }

            }
            // if property2 is Integer or String, property2 wins
            if ( (property2 instanceof Integer)
                || (property2 instanceof String) )
            {
                // property2 wins
                return -1 * direction;
            }

            // we know nothing about the property types, make them equal
            return 0;
        }
    }
}