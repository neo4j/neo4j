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

import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.storageengine.api.txstate.PrimitiveLongDiffSetsVisitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.iterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.resourceIterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.toSet;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class PrimitiveLongDiffSetsTest
{

    @Test
    public void newDiffSetIsEmpty()
    {
        assertTrue( createDiffSet().isEmpty() );
    }

    @Test
    public void addElementsToDiffSets()
    {
        PrimitiveLongDiffSets diffSets = createDiffSet();

        diffSets.add( 1L );
        diffSets.add( 2L );

        assertEquals( asSet( 1L, 2L ), toSet( diffSets.getAdded() ) );
        assertTrue( diffSets.getRemoved().isEmpty() );
        assertFalse( diffSets.isEmpty() );
    }

    @Test
    public void removeElementsInDiffSets()
    {
        PrimitiveLongDiffSets diffSets = createDiffSet();

        diffSets.remove( 1L );
        diffSets.remove( 2L );

        assertFalse( diffSets.isEmpty() );
        assertEquals( asSet( 1L, 2L ), toSet( diffSets.getRemoved() ) );
    }

    @Test
    public void removeAndAddElementsToDiffSets()
    {
        PrimitiveLongDiffSets diffSets = createDiffSet();

        diffSets.remove( 1L );
        diffSets.remove( 2L );
        diffSets.add( 1L );
        diffSets.add( 2L );
        diffSets.add( 3L );
        diffSets.remove( 4L );

        assertFalse( diffSets.isEmpty() );
        assertEquals( asSet( 4L ), toSet( diffSets.getRemoved() ) );
        assertEquals( asSet( 3L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void checkIsElementsAddedOrRemoved()
    {
        PrimitiveLongDiffSets diffSet = createDiffSet();

        diffSet.add( 1L );

        assertTrue( diffSet.isAdded( 1L ) );
        assertFalse( diffSet.isRemoved( 1L ) );

        diffSet.remove( 2L );

        assertFalse( diffSet.isAdded( 2L ) );
        assertTrue( diffSet.isRemoved( 2L ) );

        assertFalse( diffSet.isAdded( 3L ) );
        assertFalse( diffSet.isRemoved( 3L ) );
    }

    @Test
    public void addAllElements()
    {
        PrimitiveLongDiffSets diffSet = createDiffSet();

        diffSet.addAll( PrimitiveLongCollections.iterator( 7L, 8L ) );
        diffSet.addAll( PrimitiveLongCollections.iterator( 9L, 10L ) );

        assertEquals( asSet( 7L, 8L, 9L, 10L ), toSet( diffSet.getAdded() ) );
    }

    @Test
    public void removeAllElements()
    {
        PrimitiveLongDiffSets diffSet = createDiffSet();

        diffSet.removeAll( PrimitiveLongCollections.iterator( 7L, 8L ) );
        diffSet.removeAll( PrimitiveLongCollections.iterator( 9L, 10L ) );

        assertEquals( asSet( 7L, 8L, 9L, 10L ), toSet( diffSet.getRemoved() ) );
    }

    @Test
    public void addedAndRemovedElementsDelta()
    {
        PrimitiveLongDiffSets diffSet = createDiffSet();
        assertEquals( 0, diffSet.delta() );

        diffSet.addAll( PrimitiveLongCollections.iterator( 7L, 8L ) );
        diffSet.addAll( PrimitiveLongCollections.iterator( 9L, 10L ) );
        assertEquals( 4, diffSet.delta() );

        diffSet.removeAll( PrimitiveLongCollections.iterator( 8L, 9L ) );
        assertEquals( 2, diffSet.delta() );
    }

    @Test
    public void augmentDiffSetWithExternalElements()
    {
        PrimitiveLongDiffSets diffSet = createDiffSet();
        diffSet.addAll( PrimitiveLongCollections.iterator( 9L, 10L, 11L ) );
        diffSet.removeAll( PrimitiveLongCollections.iterator( 1L, 2L ) );

        PrimitiveLongIterator augmentedIterator = diffSet.augment( resourceIterator( iterator( 5L, 6L ), Resource.EMPTY) );
        assertEquals( asSet( 5L, 6L, 9L, 10L, 11L ), toSet( augmentedIterator ) );
    }

    @Test
    public void visitAddedAndRemovedElements() throws ConstraintValidationException
    {
        PrimitiveLongDiffSets diffSet = createDiffSet();
        diffSet.addAll( PrimitiveLongCollections.iterator( 9L, 10L, 11L ) );
        diffSet.removeAll( PrimitiveLongCollections.iterator( 1L, 2L ) );

        AggregatedPrimitiveLongDiffSetsVisitor visitor = new AggregatedPrimitiveLongDiffSetsVisitor();
        diffSet.visit( visitor );

        assertEquals( asSet( 9L, 10L, 11L ), toSet( visitor.getAddedElements() ) );
        assertEquals( asSet( 1L, 2L ), toSet( visitor.getRemovedElements() ) );
    }

    private static PrimitiveLongDiffSets createDiffSet()
    {
        return new PrimitiveLongDiffSets();
    }

    private static class AggregatedPrimitiveLongDiffSetsVisitor implements PrimitiveLongDiffSetsVisitor
    {
        private final PrimitiveLongSet addedElements = Primitive.longSet();
        private final PrimitiveLongSet removedElements = Primitive.longSet();

        @Override
        public void visitAdded( long element )
        {
            addedElements.add( element );
        }

        @Override
        public void visitRemoved( long element )
        {
            removedElements.add( element );
        }

        PrimitiveLongSet getAddedElements()
        {
            return addedElements;
        }

        PrimitiveLongSet getRemovedElements()
        {
            return removedElements;
        }
    }
}
