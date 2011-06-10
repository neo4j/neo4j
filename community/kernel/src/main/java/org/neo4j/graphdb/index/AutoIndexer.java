/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.neo4j.graphdb.PropertyContainer;

/**
 * The primary interaction point with the auto indexing infrastructure of neo4j.
 * From here it is possible to enable/disable the auto indexing functionality,
 * set/unset auto indexed properties and retrieve index hits.
 */
public interface AutoIndexer<T extends PropertyContainer>
{
    /**
     * Sets the AutoIndexer as enabled or not. Enabled means that appropriately
     * configured properties are auto indexed and hits can be returned, disabled
     * means that no index additions happen but the index can be queried.
     *
     * @param enabled True to enable this auto indexer, false to disable it.
     */
    void setAutoIndexingEnabled( boolean enabled );

    /**
     * Returns true iff this auto indexer is enabled, false otherwise. For a
     * cursory definition of enabled indexer, look at
     * <code>setAutoIndexingEnabled(boolean)</code>
     *
     * @return true iff this auto indexer is enabled
     *
     * @see setAutoIndexingEnabled(boolean)
     */
    boolean isAutoIndexingEnabled();

    /**
     * Returns the index used by the auto indexer for indexing properties.
     *
     * @return A read only index
     */
    AutoIndex<T> getAutoIndex();

    void startAutoIndexingProperty( String propName );

    void stopAutoIndexingProperty( String propName );

    void startIgnoringProperty( String propName );

    void stopIgnoringProperty( String propName );
}
