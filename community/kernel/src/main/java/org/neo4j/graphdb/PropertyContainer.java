/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
 * Defines a common API for handling properties on both {@link Node nodes} and
 * {@link Relationship relationships}.
 * <p>
 * Properties are key-value pairs. The keys are always strings. Valid property
 * value types are all the Java primitives (<code>int</code>, <code>byte</code>,
 * <code>float</code>, etc), <code>java.lang.String</code>s and arrays of
 * primitives and Strings.
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
     * one of the valid property types, i.e. a Java primitive, a {@link String
     * String} or an array of any of the valid types.
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
     * value. The value is of one of the valid property types, i.e. a Java
     * primitive, a {@link String String} or an array of any of the valid types.
     *
     * @param key the property key
     * @param defaultValue the default value that will be returned if no
     *            property value was associated with the given key
     * @return the property value associated with the given key
     */
    Object getProperty( String key, Object defaultValue );

    /**
     * Sets the property value for the given key to <code>value</code>. The
     * property value must be one of the valid property types, i.e:
     * <ul>
     * <li><code>boolean</code> or <code>boolean[]</code></li>
     * <li><code>byte</code> or <code>byte[]</code></li>
     * <li><code>short</code> or <code>short[]</code></li>
     * <li><code>int</code> or <code>int[]</code></li>
     * <li><code>long</code> or <code>long[]</code></li>
     * <li><code>float</code> or <code>float[]</code></li>
     * <li><code>double</code> or <code>double[]</code></li>
     * <li><code>char</code> or <code>char[]</code></li>
     * <li><code>java.lang.String</code> or <code>String[]</code></li>
     * </ul>
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
}
