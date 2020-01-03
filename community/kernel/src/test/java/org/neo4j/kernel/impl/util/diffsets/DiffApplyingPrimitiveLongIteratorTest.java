/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.util.diffsets;

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Set;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Resource;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.neo4j.collection.PrimitiveLongCollections.asArray;
import static org.neo4j.collection.PrimitiveLongCollections.iterator;
import static org.neo4j.collection.PrimitiveLongCollections.resourceIterator;
import static org.neo4j.collection.PrimitiveLongCollections.toSet;

public class DiffApplyingPrimitiveLongIteratorTest
{
    @Test
    public void iterateOnlyOverAddedElementsWhenSourceIsEmpty()
    {
        LongIterator emptySource = ImmutableEmptyLongIterator.INSTANCE;
        LongSet added = LongHashSet.newSetWith( 1L, 2L );
        LongSet removed = LongHashSet.newSetWith( 3L );

        LongIterator iterator = DiffApplyingPrimitiveLongIterator.augment( emptySource, added, removed );
        Set<Long> resultSet = toSet( iterator );
        assertThat( resultSet, containsInAnyOrder(1L, 2L) );
    }

    @Test
    public void appendSourceElementsDuringIteration()
    {
        LongIterator source = iterator( 4L, 5L );
        LongSet added = LongHashSet.newSetWith( 1L, 2L );
        LongSet removed = LongHashSet.newSetWith( 3L );

        LongIterator iterator = DiffApplyingPrimitiveLongIterator.augment( source, added, removed );
        Set<Long> resultSet = toSet( iterator );
        assertThat( resultSet, containsInAnyOrder(1L, 2L, 4L, 5L) );
    }

    @Test
    public void doNotIterateTwiceOverSameElementsWhenItsPartOfSourceAndAdded()
    {
        LongIterator source = iterator( 4L, 5L );
        LongSet added = LongHashSet.newSetWith( 1L, 4L );
        LongSet removed = LongHashSet.newSetWith( 3L );

        LongIterator iterator = DiffApplyingPrimitiveLongIterator.augment( source, added, removed );
        Long[] values = ArrayUtils.toObject( asArray( iterator ) );
        assertThat( values, arrayContainingInAnyOrder( 1L, 4L, 5L ) );
        assertThat( values, arrayWithSize( 3 ) );
    }

    @Test
    public void doNotIterateOverDeletedElement()
    {
        LongIterator source = iterator( 3L, 5L );
        LongSet added = LongHashSet.newSetWith( 1L );
        LongSet removed = LongHashSet.newSetWith( 3L );

        LongIterator iterator = DiffApplyingPrimitiveLongIterator.augment( source, added, removed );
        Set<Long> resultSet = toSet( iterator );
        assertThat( resultSet, containsInAnyOrder(1L, 5L) );
    }

    @Test
    public void closeResource()
    {
        Resource resource = Mockito.mock( Resource.class );
        PrimitiveLongResourceIterator source = resourceIterator( ImmutableEmptyLongIterator.INSTANCE, resource );

        PrimitiveLongResourceIterator iterator = DiffApplyingPrimitiveLongIterator.augment( source, LongSets.immutable.empty(), LongSets.immutable.empty() );

        iterator.close();

        Mockito.verify( resource ).close();
    }
}
