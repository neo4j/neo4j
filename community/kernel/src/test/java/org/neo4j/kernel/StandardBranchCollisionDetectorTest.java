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
package org.neo4j.kernel;

import com.google.common.collect.Iterators;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;

import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalBranch;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StandardBranchCollisionDetectorTest
{

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testFilteredPathEvaluation()
    {
        final PropertyContainer endNode = mock( Node.class );
        final PropertyContainer alternativeEndNode = mock( Node.class );
        final Node startNode = mock( Node.class );
        Evaluator evaluator = mock( Evaluator.class );
        TraversalBranch branch = mock( TraversalBranch.class );
        TraversalBranch alternativeBranch = mock( TraversalBranch.class );

        when( branch.iterator() ).thenAnswer( new IteratorAnswer( endNode ) );
        when( alternativeBranch.iterator() ).thenAnswer( new IteratorAnswer( alternativeEndNode ) );
        when( alternativeBranch.startNode() ).thenReturn( startNode );
        when( evaluator.evaluate( Mockito.any( Path.class ) ) ).thenReturn( Evaluation.INCLUDE_AND_CONTINUE );
        StandardBranchCollisionDetector collisionDetector = new StandardBranchCollisionDetector( evaluator,
                new Predicate<Path>()
                {
                    @Override
                    public boolean test( Path path )
                    {
                        return alternativeEndNode.equals( path.endNode() ) && startNode.equals( path.startNode() );
                    }
                } );

        Collection<Path> incoming = collisionDetector.evaluate( branch, Direction.INCOMING );
        Collection<Path> outgoing = collisionDetector.evaluate( branch, Direction.OUTGOING );
        Collection<Path> alternativeIncoming = collisionDetector.evaluate( alternativeBranch, Direction.INCOMING );
        Collection<Path> alternativeOutgoing = collisionDetector.evaluate( alternativeBranch, Direction.OUTGOING );

        assertThat( incoming, nullValue() );
        assertThat( outgoing, nullValue() );
        assertThat( alternativeIncoming, nullValue() );
        assertThat( alternativeOutgoing, hasSize( 1 ) );
    }

    private static class IteratorAnswer implements Answer<Object>
    {
        private final PropertyContainer endNode;

        public IteratorAnswer( PropertyContainer endNode )
        {
            this.endNode = endNode;
        }

        @Override
        public Object answer( InvocationOnMock invocation ) throws Throwable
        {
            return Iterators.singletonIterator( endNode );
        }
    }
}
