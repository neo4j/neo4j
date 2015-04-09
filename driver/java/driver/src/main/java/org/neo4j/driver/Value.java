/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver;

/**
 * Represents a value from Neo4j.
 * <p>
 * This interface describes a number of <code>isType</code> methods along with
 * <code>typeValue</code> methods. The first set of these correlate with types from
 * the Neo4j Type System and are used to determine which Neo4j type is represented.
 * The second set of methods perform coercions to Java types (wherever possible).
 * For example, a common Text value should be tested for using <code>isText</code>
 * and extracted using <code>stringValue</code>.
 * <p>
 * <h2>Navigating a tree structure</h2>
 * <p>
 * Because Neo4j often handles dynamic structures, this interface is designed to help
 * you handle such structures in Java. Specifically, {@link org.neo4j.driver.Value} lets you navigate arbitrary tree
 * structures without having to resort to type casting.
 * <p>
 * Given a tree structure like:
 * <p>
 * <pre>
 * {@code
 * {
 *   users : [
 *     { name : "Anders" },
 *     { name : "John" }
 *   ]
 * }
 * }
 * </pre>
 * <p>
 * You can retrieve the name of the second user, John, like so:
 * <p>
 * <pre>
 * {@code
 * String username = value.get("users").get(1).get("name").stringValue();
 * }
 * </pre>
 * <p>
 * You can also easily iterate over the users:
 * <p>
 * <pre>
 * {@code
 * for(Value user : value.get("users") )
 * {
 *     System.out.println(user.get("name").stringValue());
 * }
 * }
 * </pre>
 */
public interface Value extends Iterable<Value>
{
    /** @return the value as a Java String, if possible. */
    String javaString();

    /** @return the value as a Java int, if possible. */
    int javaInteger();

    /** @return the value as a Java long, if possible. */
    long javaLong();

    /** @return the value as a Java float, if possible. */
    float javaFloat();

    /** @return the value as a Java double, if possible. */
    double javaDouble();

    /** @return the value as a Java boolean, if possible. */
    boolean javaBoolean();

    /** @return the value as an {@link Identity}, if possible. */
    Identity asIdentity();

    /** @return the value as a {@link Node}, if possible. */
    Node asNode();

    /** @return the value as a {@link Relationship}, if possible. */
    Relationship asRelationship();

    /** @return the value as a {@link Path}, if possible. */
    Path asPath();

    /**
     * Retrieve an inner value by index. This can be used for {@link #isList() lists}.
     *
     * @param index the index to look up a value by
     * @return the value at the specified index
     */
    Value get( long index );

    /**
     * Retrieve an inner value by key. This can be used for {@link #isMap() maps},
     * {@link #isNode() nodes} and {@link #isRelationship() relationships}. For nodes and relationships, this method
     * returns property values.
     *
     * @param key the key to find a value by
     * @return the value with the specified key
     */
    Value get( String key );

    /**
     * If the underlying value is a collection type, return the number of values in the collection.
     * <p>
     * For {@link #isList() list} values, this will return the size of the list.
     * <p>
     * For {@link #isMap() map} values, this will return the number of entries in the map.
     * <p>
     * For {@link #isNode() node} and {@link #isRelationship() relationship} values,
     * this will return the number of properties.
     * <p>
     * For {@link #isPath() path} values, this returns the length (number of relationships) in the path.
     *
     * @return the number of values in an underlying collection
     */
    long size();

    /**
     * If the underlying value supports {@link #get(String) key-based indexing}, return an iterable of the keys in the
     * map, this applies to {@link #isMap() map}, {@link #asNode() node} and {@link
     * #isRelationship() relationship} values.
     *
     * @return the keys in the value
     */
    Iterable<String> keys();

    /** @return true if the underlying value is a Neo4j text value */
    boolean isText();

    /** @return if the underlying value is a Neo4j 64-bit integer */
    boolean isInteger();

    /** @return if the underlying value is a Neo4j 64-bit float */
    boolean isFloat();

    /** @return if the underlying value is a Neo4j boolean */
    boolean isBoolean();

    /** @return if the underlying value is a Neo4j identity */
    boolean isIdentity();

    /** @return if the underlying value is a Neo4j node */
    boolean isNode();

    /** @return if the underlying value is a Neo4j path */
    boolean isPath();

    /** @return if the underlying value is a Neo4j relationship */
    boolean isRelationship();

    /**
     * Lists are an ordered collection of values. You can {@link #iterator() iterate} over a list as well as
     * access specific values {@link #get(long) by index}.
     * <p>
     * {@link #size()} will give you the number of entries in the list.
     *
     * @return if the underlying value is a Neo4j list
     */
    boolean isList();

    /**
     * Maps are key/value objects, similar to {@link java.util.Map java maps}. You can use {@link #get(String)} to
     * retrive values from the map, {@link #keys()} to list keys and {@link #iterator()} to iterate over the values.
     * <p>
     * {@link #size()} will give you the number of entries in the map.
     *
     * @return if the underlying value is a Neo4j map
     */
    boolean isMap();
}
