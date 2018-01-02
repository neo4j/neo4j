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
package org.neo4j.kernel.impl.api;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertThat;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.iterator;
import static org.neo4j.graphdb.Neo4jMatchers.hasSamePrimitiveItems;

import java.util.Iterator;

import org.junit.Test;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;

public class LongDiffSetsTest
{

    // TODO empty diffset EMPTY
    // TODO null/isEmpty elements

    @Test
    public void shouldContainSourceForEmptyDiffSets() throws Exception
    {
        // given
        DiffSets<Long> diffSets = new DiffSets<>( );
        Iterator<Long> expected = diffSets.apply( iteratorSource( 1l, 2l ) );

        // when
        PrimitiveLongIterator actual = diffSets.augment( iterator( 1l, 2l ) );

        // then
        assertThat( expected, hasSamePrimitiveItems( actual ) );
    }

    @Test
    public void shouldContainFilteredSourceForDiffSetsWithRemovedElements() throws Exception
    {
        // given
        DiffSets<Long> diffSets = new DiffSets<>( );
        diffSets.remove( 17l );
        diffSets.remove( 18l );
        Iterator<Long> expected = diffSets.apply( iteratorSource( 1L, 17l, 3l ) );

        // when
        PrimitiveLongIterator actual = diffSets.augment( iterator( 1l, 17l, 3l ) );

        // then
        assertThat( expected, hasSamePrimitiveItems( actual ) );
    }

    @Test
    public void shouldContainFilteredSourceForDiffSetsWithAddedElements() throws Exception
    {
        // given
        DiffSets<Long> diffSets = new DiffSets<>( );
        diffSets.add( 17l );
        diffSets.add( 18l );
        Iterator<Long> expected = diffSets.apply( iteratorSource( 1L, 17l, 3l ) );

        // when
        PrimitiveLongIterator actual = diffSets.augment( iterator( 1l, 17l, 3l ) );

        // then
        assertThat( expected, hasSamePrimitiveItems( actual ) );
    }

    @Test
    public void shouldContainAddedElementsForDiffSetsWithAddedElements() throws Exception
    {
        // given
        DiffSets<Long> diffSets = new DiffSets<>( );
        diffSets.add( 19l );
        diffSets.add( 20l );
        Iterator<Long> expected = diffSets.apply( iteratorSource( 19l ) );

        // when
        PrimitiveLongIterator actual = diffSets.augment( iterator( 19l ) );

        // then
        assertThat( expected, hasSamePrimitiveItems( actual ) );
    }

    private static Iterator<Long> iteratorSource( Long... values )
    {
        return asList( values ).iterator();
    }
}
