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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertThat;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.iterator;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasSamePrimitiveItems;

public class LongDiffSetsTest
{

    // TODO empty diffset EMPTY
    // TODO null/isEmpty elements

    @Test
    public void shouldContainSourceForEmptyDiffSets()
    {
        // given
        DiffSets<Long> diffSets = new DiffSets<>( );
        Iterator<Long> expected = diffSets.apply( iteratorSource( 1L, 2L ) );

        // when
        PrimitiveLongIterator actual = diffSets.augment( iterator( 1L, 2L ) );

        // then
        assertThat( expected, hasSamePrimitiveItems( actual ) );
    }

    @Test
    public void shouldContainFilteredSourceForDiffSetsWithRemovedElements()
    {
        // given
        DiffSets<Long> diffSets = new DiffSets<>( );
        diffSets.remove( 17L );
        diffSets.remove( 18L );
        Iterator<Long> expected = diffSets.apply( iteratorSource( 1L, 17L, 3L ) );

        // when
        PrimitiveLongIterator actual = diffSets.augment( iterator( 1L, 17L, 3L ) );

        // then
        assertThat( expected, hasSamePrimitiveItems( actual ) );
    }

    @Test
    public void shouldContainFilteredSourceForDiffSetsWithAddedElements()
    {
        // given
        DiffSets<Long> diffSets = new DiffSets<>( );
        diffSets.add( 17L );
        diffSets.add( 18L );
        Iterator<Long> expected = diffSets.apply( iteratorSource( 1L, 17L, 3L ) );

        // when
        PrimitiveLongIterator actual = diffSets.augment( iterator( 1L, 17L, 3L ) );

        // then
        assertThat( expected, hasSamePrimitiveItems( actual ) );
    }

    @Test
    public void shouldContainAddedElementsForDiffSetsWithAddedElements()
    {
        // given
        DiffSets<Long> diffSets = new DiffSets<>( );
        diffSets.add( 19L );
        diffSets.add( 20L );
        Iterator<Long> expected = diffSets.apply( iteratorSource( 19L ) );

        // when
        PrimitiveLongIterator actual = diffSets.augment( iterator( 19L ) );

        // then
        assertThat( expected, hasSamePrimitiveItems( actual ) );
    }

    private static Iterator<Long> iteratorSource( Long... values )
    {
        return asList( values ).iterator();
    }
}
