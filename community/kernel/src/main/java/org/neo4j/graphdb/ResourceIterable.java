/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
 * Iterable whose {@link ResourceIterator iterators} have associated resources that must be managed.
 *
 * @param <T> the type of values returned through the iterators.
 * @see ResourceIterator
 */
public interface ResourceIterable<T> extends Iterable<T>
{
    /**
     * Returns an {@link ResourceIterator iterator} with associated resources that must be managed.
     *
     * Don't forget to either exhaust the returned iterator or call the
     * {@link ResourceIterator#close() close method} on it.
     */
    @Override
    ResourceIterator<T> iterator();
}
