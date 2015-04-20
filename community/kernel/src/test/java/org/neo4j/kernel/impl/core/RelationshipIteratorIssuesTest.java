/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.junit.Test;

import org.neo4j.kernel.impl.api.store.CacheUpdateListener;
import org.neo4j.kernel.impl.core.NodeImpl.LoadStatus;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdIterator;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper.OUTGOING;

/**
 * @see ConcurrentCreateAndGetRelationshipsIT for integration/probabilistic test
 */
public class RelationshipIteratorIssuesTest
{
    @Test
    public void arrayIndexOutOfBoundsInRelTypeArrayWhenCreatingRelationshipsConcurrently() throws Exception
    {
        // GIVEN
        // -- a node manager capable of serving light-weight RelationshipProxy capable of answering getId()
        CacheUpdateListener cacheUpdateListener = mock( CacheUpdateListener.class );
        RelationshipLoader loader = mock( RelationshipLoader.class );

        // -- a node that says it cannot load any more relationships
        NodeImpl node = mock( NodeImpl.class );
        when( node.getMoreRelationships( eq( loader ), any( DirectionWrapper.class ),
                any( int[].class ), any( CacheUpdateListener.class ) ) ).thenReturn( LoadStatus.NOTHING );

        // -- a type iterator that at this point contains one relationship (0)
        ControlledRelIdIterator typeIterator = new ControlledRelIdIterator( 0L );
        RelationshipIterator iterator = new RelationshipIterator( new RelIdIterator[] { typeIterator },
                node, OUTGOING, new int[0], loader, false, false, cacheUpdateListener );
        // -- go forth one step in the iterator
        iterator.next();

        // WHEN
        // -- one relationship has been returned, and we're in the middle of the next call to next()
        //    typeIterator will get one more relationship in it. To mimic this we control the outcome of
        //    RelIdIterator#hasNext() so that we get to the correct branch in the RelationshipIterator code
        typeIterator.queueHasNextAnswers( false, false, true );
        long otherRelationship = 1, thirdRelationship = 2;
        typeIterator.add( otherRelationship, thirdRelationship );

        // -- go one more step, getting us into the state where the type index in RelationshipIterator
        //    was incremented by mistake. Although this particular call to next() succeeds
        iterator.next();

        // -- call next() again, where the first thing happening is to get the RelIdIterator with the
        //    now invalid type index, causing ArrayIndexOutOfBoundsException
        long returnedThirdRelationship = iterator.next();

        // THEN
        assertEquals( thirdRelationship, returnedThirdRelationship );
    }

    private static class ControlledRelIdIterator implements RelIdIterator
    {
        private final List<Long> ids;
        private int index;
        private final Queue<Boolean> controlledHasNextResults = new LinkedList<>();

        ControlledRelIdIterator( Long... ids )
        {
            this.ids = new ArrayList<>( Arrays.asList( ids ) );
        }

        void add( long... ids )
        {
            for ( long id : ids )
            {
                this.ids.add( id );
            }
        }

        void queueHasNextAnswers( boolean... answers )
        {
            for ( boolean answer : answers )
            {
                controlledHasNextResults.add( answer );
            }
        }

        @Override
        public int getType()
        {
            return 0;
        }

        @Override
        public boolean hasNext()
        {
            Boolean controlledAnswer = controlledHasNextResults.poll();
            if ( controlledAnswer != null )
            {
                return controlledAnswer.booleanValue();
            }

            return index < ids.size();
        }

        @Override
        public long next()
        {
            if ( !hasNext() )
            {
                throw new NoSuchElementException();
            }
            return ids.get( index++ );
        }

        @Override
        public RelIdIterator updateSource( RelIdArray newSource, DirectionWrapper direction )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void doAnotherRound()
        {
            throw new UnsupportedOperationException();
        }
    }
}
