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
package org.neo4j.collection.primitive;

import org.neo4j.function.IntPredicate;
import org.neo4j.function.primitive.PrimitiveIntPredicate;

public interface PrimitiveIntSet extends PrimitiveIntCollection, IntPredicate, PrimitiveIntPredicate
{
    boolean add( int value );

    boolean addAll( PrimitiveIntIterator values );

    boolean contains( int value );

    boolean remove( int value );

    /**
     * @deprecated use {@link #contains(int)} instead, or {@link #test(int)} if a predicate is required
     */
    @Deprecated
    @Override
    boolean accept( int value );
}
