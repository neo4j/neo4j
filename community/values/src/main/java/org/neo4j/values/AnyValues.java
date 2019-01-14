/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.values;

import java.util.Comparator;

import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.VirtualValueGroup;

@SuppressWarnings( "WeakerAccess" )
public final class AnyValues
{
    /**
     * Default AnyValue comparator. Will correctly compare all storable and virtual values.
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
     *   <ul>
     *     <li> Regular map
     *
     *     <li> NODE
     *
     *     <li> RELATIONSHIP
     *   </ul>
     *
     *  <li> LIST OF ANY?
     *
     *  <li> PATH
     *
     *  <li> POINT
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
     */
    private static final AnyValueComparator comp = new AnyValueComparator( Values.COMPARATOR, VirtualValueGroup::compareTo );
    public static final Comparator<AnyValue> COMPARATOR = comp;
    public static final TernaryComparator<AnyValue> TERNARY_COMPARATOR = comp;

}
