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
package org.neo4j.helpers;

/**
 * Predicate useful for filtering.
 *
 * @param <T> type of items
 * @deprecated use {@link org.neo4j.function.Predicate} instead
 */
@Deprecated
public interface Predicate<T>
{
    /**
     * Decide whether or not to accept an item. 
     * 
     * @param item item to accept or not
     * @return whether or not to accept the {@code item}, where {@code true}
     * means that the {@code item} is accepted and {@code false} means that
     * it's not (i.e. didn't pass the filter).
     */
    boolean accept( T item );
}
