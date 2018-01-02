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
package org.neo4j.graphdb;

/**
 * A label is a grouping facility for {@link Node} where all nodes having a label
 * are part of the same group. Labels on nodes are optional and any node can
 * have an arbitrary number of labels attached to it.
 *
 * Objects of classes implementing this interface can be used as label
 * representations in your code.
 *
 * It's very important to note that a label is uniquely identified
 * by its name, not by any particular instance that implements this interface.
 * This means that the proper way to check if two labels are equal
 * is by invoking <code>equals()</code> on their {@link #name() names}, NOT by
 * using Java's identity operator (<code>==</code>) or <code>equals()</code> on
 * the {@link Label} instances. A consequence of this is that you can NOT
 * use {@link Label} instances in hashed collections such as
 * {@link java.util.HashMap HashMap} and {@link java.util.HashSet HashSet}.
 * <p>
 * However, you usually want to check whether a specific node
 * <i>instance</i> has a certain label. That is best achieved with the
 * {@link Node#hasLabel(Label)} method.
 * 
 * For labels that your application know up front you should specify using an enum,
 * and since the name is accessed using the {@link #name()} method it fits nicely.
 * <code>
 * public enum MyLabels implements Label
 * {
 *     PERSON,
 *     RESTAURANT;
 * }
 * </code>
 * 
 * For labels that your application don't know up front you can make use of
 * {@link DynamicLabel#label(String)}, or your own implementation of this interface,
 * as it's just the name that matters.
 *
 * @see DynamicLabel
 * @see Node
 */
public interface Label
{
    /**
     * Returns the name of the label. The name uniquely identifies a
     * label, i.e. two different Label instances with different object identifiers
     * (and possibly even different classes) are semantically equivalent if they
     * have {@link String#equals(Object) equal} names.
     *
     * @return the name of the label
     */
    String name();
}
