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
package org.neo4j.graphdb;

import java.util.Map;

/**
 * Defines a common API for handling properties on both {@link Node nodes} and
 * {@link Relationship relationships}.
 * <p>
 * Properties are key-value pairs. The keys are always strings. Valid property
 * value types are all the Java primitives (<code>int</code>, <code>byte</code>,
 * <code>float</code>, etc), <code>java.lang.String</code>s, the <em>Spatial</em>
 * and <em>Temporal</em> types and arrays of any of these.
 * <p>
 * The complete list of currently supported property types is:
 * <ul>
 * <li><code>boolean</code></li>
 * <li><code>byte</code></li>
 * <li><code>short</code></li>
 * <li><code>int</code></li>
 * <li><code>long</code></li>
 * <li><code>float</code></li>
 * <li><code>double</code></li>
 * <li><code>char</code></li>
 * <li><code>java.lang.String</code></li>
 * <li><code>org.neo4j.graphdb.spatial.Point</code></li>
 * <li><code>java.time.LocalDate</code></li>
 * <li><code>java.time.OffsetTime</code></li>
 * <li><code>java.time.LocalTime</code></li>
 * <li><code>java.time.ZonedDateTime</code><br>
 * <div style="padding-left: 20pt;">It is also possible to use <code>java.time.OffsetDateTime</code> and it will
 * be converted to a <code>ZonedDateTime</code> internally.</div>
 * </li>
 * <li><code>java.time.LocalDateTime</code></li>
 * <li><code>java.time.temporal.TemporalAmount</code><br>
 * <div style="padding-left: 20pt;">There are two concrete implementations of this interface, <code>java.time.Duration</code>
 * and <code>java.time.Period</code> which will be converted to a single Neo4j <code>Duration</code>
 * type. This means loss of type information, so properties of this type, when read back using
 * {@link #getProperty(String) getProperty} will be only of type <code>java.time.temporal.TemporalAmount</code>.</div>
 * </li>
 * <li>Arrays of any of the above types, for example <code>int[]</code>, <code>String[]</code> or <code>LocalTime[]</code></li>
 * </ul>
 * <p>
 * <b>Please note</b> that Neo4j does NOT accept arbitrary objects as property
 * values. {@link #setProperty(String, Object) setProperty()} takes a
 * <code>java.lang.Object</code> only to avoid an explosion of overloaded
 * <code>setProperty()</code> methods.
 */
public interface PropertyContainer
{
    /**
     * Get the {@link GraphDatabaseService} that this {@link Node} or
     * {@link Relationship} belongs to.
     *
     * @return The GraphDatabase this Node or Relationship belongs to.
     */
    GraphDatabaseService getGraphDatabase();

    /**
     * Returns <code>true</code> if this property container has a property
     * accessible through the given key, <code>false</code> otherwise. If key is
     * <code>null</code>, this method returns <code>false</code>.
     *
     * @param key the property key
     * @return <code>true</code> if this property container has a property
     *         accessible through the given key, <code>false</code> otherwise
     */
    boolean hasProperty( String key );

    /**
     * Returns the property value associated with the given key. The value is of
     * one of the valid property types, i.e. a Java primitive,
     * a {@link String String}, a {@link org.neo4j.graphdb.spatial.Point Point},
     * a valid temporal type, or an array of any of the valid types.
     * See the {@link PropertyContainer the class description}
     * for a full list of known types.
     * <p>
     * If there's no property associated with <code>key</code> an unchecked
     * exception is raised. The idiomatic way to avoid an exception for an
     * unknown key and instead get <code>null</code> back is to use a default
     * value: {@link #getProperty(String, Object) Object valueOrNull =
     * nodeOrRel.getProperty( key, null )}
     *
     * @param key the property key
     * @return the property value associated with the given key
     * @throws NotFoundException if there's no property associated with
     *             <code>key</code>
     */
    Object getProperty( String key );

    /**
     * Returns the property value associated with the given key, or a default
     * value. The value is of one of the valid property types, i.e. a Java primitive,
     * a {@link String String}, a {@link org.neo4j.graphdb.spatial.Point Point},
     * a valid temporal type, or an array of any of the valid types.
     * See the {@link PropertyContainer the class description}
     * for a full list of known types.
     *
     * @param key the property key
     * @param defaultValue the default value that will be returned if no
     *            property value was associated with the given key
     * @return the property value associated with the given key
     */
    Object getProperty( String key, Object defaultValue );

    /**
     * Sets the property value for the given key to <code>value</code>. The
     * property value must be one of the valid property types, i.e. a Java primitive,
     * a {@link String String}, a {@link org.neo4j.graphdb.spatial.Point Point},
     * a valid temporal type, or an array of any of the valid types.
     * See the {@link PropertyContainer the class description}
     * for a full list of known types.
     * <p>
     * This means that <code>null</code> is not an accepted property value.
     *
     * @param key the key with which the new property value will be associated
     * @param value the new property value, of one of the valid property types
     * @throws IllegalArgumentException if <code>value</code> is of an
     *             unsupported type (including <code>null</code>)
     */
    void setProperty( String key, Object value );

    /**
     * Removes the property associated with the given key and returns the old
     * value. If there's no property associated with the key, <code>null</code>
     * will be returned.
     *
     * @param key the property key
     * @return the property value that used to be associated with the given key
     */
    Object removeProperty( String key );

    /**
     * Returns all existing property keys, or an empty iterable if this property
     * container has no properties.
     *
     * @return all property keys on this property container
     */
    // TODO: figure out concurrency semantics
    Iterable<String> getPropertyKeys();

    /**
     * Returns specified existing properties. The collection is mutable,
     * but changing it has no impact on the graph as the data is detached.
     *
     * @param keys the property keys to return
     * @return specified properties on this property container
     * @throws NullPointerException if the array of keys or any key is null
     */
    Map<String, Object> getProperties( String... keys );

    /**
     * Returns all existing properties.
     *
     * @return all properties on this property container
     */
    Map<String, Object> getAllProperties();
}
