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
package org.neo4j.kernel.impl.util.diffsets;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Resource;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.asArray;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptyIterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptySet;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.iterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.resourceIterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.setOf;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.toSet;

public class DiffApplyingPrimitiveLongIteratorTest
{
    @Test
    public void iterateOnlyOverAddedElementsWhenSourceIsEmpty()
    {
        PrimitiveLongIterator emptySource = emptyIterator();
        PrimitiveLongSet added = setOf( 1L, 2L );
        PrimitiveLongSet removed = setOf( 3L );

        PrimitiveLongIterator iterator = DiffApplyingPrimitiveLongIterator.augment( emptySource, added, removed );
        Set<Long> resultSet = toSet( iterator );
        assertThat( resultSet, containsInAnyOrder(1L, 2L) );
    }

    @Test
    public void appendSourceElementsDuringIteration()
    {
        PrimitiveLongIterator source = iterator( 4L, 5L );
        PrimitiveLongSet added = setOf( 1L, 2L );
        PrimitiveLongSet removed = setOf( 3L );

        PrimitiveLongIterator iterator = DiffApplyingPrimitiveLongIterator.augment( source, added, removed );
        Set<Long> resultSet = toSet( iterator );
        assertThat( resultSet, containsInAnyOrder(1L, 2L, 4L, 5L) );
    }

    @Test
    public void doNotIterateTwiceOverSameElementsWhenItsPartOfSourceAndAdded()
    {
        PrimitiveLongIterator source = iterator( 4L, 5L );
        PrimitiveLongSet added = setOf( 1L, 4L );
        PrimitiveLongSet removed = setOf( 3L );

        PrimitiveLongIterator iterator = DiffApplyingPrimitiveLongIterator.augment( source, added, removed );
        Long[] values = ArrayUtils.toObject( asArray( iterator ) );
        assertThat( values, arrayContainingInAnyOrder( 1L, 4L, 5L ) );
        assertThat( values, arrayWithSize( 3 ) );
    }

    @Test
    public void doNotIterateOverDeletedElement()
    {
        PrimitiveLongIterator source = iterator( 3L, 5L );
        PrimitiveLongSet added = setOf( 1L );
        PrimitiveLongSet removed = setOf( 3L );

        PrimitiveLongIterator iterator = DiffApplyingPrimitiveLongIterator.augment( source, added, removed );
        Set<Long> resultSet = toSet( iterator );
        assertThat( resultSet, containsInAnyOrder(1L, 5L) );
    }

    @Test
    public void closeResource()
    {
        Resource resource = Mockito.mock( Resource.class );
        PrimitiveLongResourceIterator source = resourceIterator( emptyIterator(), resource );

        PrimitiveLongResourceIterator iterator = DiffApplyingPrimitiveLongIterator.augment( source, emptySet(), emptySet() );

        iterator.close();

        Mockito.verify( resource ).close();
    }
}
