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
package org.neo4j.graphdb.index;

import java.util.Set;

import org.neo4j.graphdb.PropertyContainer;

/**
 * The primary interaction point with the auto indexing infrastructure of neo4j.
 * From here it is possible to enable/disable the auto indexing functionality,
 * set/unset auto indexed properties and retrieve index hits.
 *
 * It only exposes a {@link ReadableIndex} (see {@link #getAutoIndex()}) and
 * the idea is that the mutating operations are managed by the AutoIndexer only
 * and the user should have no access other than mutating operations on the
 * database primitives.
 *
 * @deprecated this feature will be removed in a future release, please consider using schema indexes instead
 */
@Deprecated
public interface AutoIndexer<T extends PropertyContainer>
{
    /**
     * Sets the AutoIndexer as enabled or not. Enabled means that appropriately
     * configured properties are auto indexed and hits can be returned, disabled
     * means that no index additions happen but the index can be queried.
     *
     * @param enabled True to enable this auto indexer, false to disable it.
     */
    void setEnabled( boolean enabled );

    /**
     * Returns true iff this auto indexer is enabled, false otherwise. For a
     * cursory definition of enabled indexer, look at
     * <code>setAutoIndexingEnabled(boolean)</code>
     *
     * @return true iff this auto indexer is enabled
     *
     * @see #setEnabled(boolean)
     */
    boolean isEnabled();

    /**
     * Returns the auto index used by the auto indexer. This should be able
     * to be released safely (read only) to the outside world.
     *
     * @return A read only index
     */
    ReadableIndex<T> getAutoIndex();

    /**
     * Start auto indexing a property. This could lead to an
     * IllegalStateException in case there are already ignored properties.
     * Adding an already auto indexed property is a no-op.
     *
     * @param propName The property name to start auto indexing.
     */
    void startAutoIndexingProperty( String propName );

    /**
     * Removes the argument from the set of auto indexed properties. If
     * the property was not already monitored, nothing happens
     *
     * @param propName The property name to stop auto indexing.
     */
    void stopAutoIndexingProperty( String propName );

    /**
     * Returns the set of property names that are currently monitored for auto
     * indexing. If this auto indexer is set to ignore properties, the result
     * is the empty set.
     *
     * @return An immutable set of the auto indexed property names, possibly
     *         empty.
     */
    Set<String> getAutoIndexedProperties();
}
