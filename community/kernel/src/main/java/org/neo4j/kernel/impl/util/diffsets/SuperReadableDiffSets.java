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
package org.neo4j.kernel.impl.util.diffsets;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.Predicate;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;

/**
 * Super class of diff sets where use of {@link PrimitiveLongIterator} can be parameterized
 * to a specific subclass instead.
 */
public interface SuperReadableDiffSets<T,LONGITERATOR extends PrimitiveLongIterator>
{
    boolean isAdded( T elem );

    boolean isRemoved( T elem );

    Set<T> getAdded();

    Set<T> getRemoved();

    boolean isEmpty();

    Iterator<T> apply( Iterator<T> source );

    int delta();

    LONGITERATOR augment( LONGITERATOR source );

    PrimitiveIntIterator augment( PrimitiveIntIterator source );

    LONGITERATOR augmentWithRemovals( LONGITERATOR source );

    SuperReadableDiffSets<T,LONGITERATOR> filterAdded( Predicate<T> addedFilter );

    void accept( DiffSetsVisitor<T> visitor )
            throws ConstraintValidationKernelException, CreateConstraintFailureException;
}
