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
package org.neo4j.kernel.impl.store;

import java.util.List;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.storageengine.api.CountsDelta;

import static org.junit.Assert.assertEquals;
import static org.neo4j.register.Registers.newDoubleLongRegister;

public class CountsOracle
{
    public static class Node
    {
        private final long[] labels;

        private Node( long[] labels )
        {
            this.labels = labels;
        }
    }

    private final CountsDelta state = new CountsDelta();

    public Node node( long... labels )
    {
        state.addNode( labels );
        return new Node( labels );
    }

    public void relationship( Node start, int type, Node end )
    {
        state.addRelationship( start.labels, type, end.labels );
    }

    public void update( CountsTracker target, long txId )
    {
        try ( CountsAccessor.Updater updater = target.apply( txId ).get() )
        {
            state.accept( new CountsAccessor.Initializer( updater ) );
        }
    }

    public void update( CountsOracle target )
    {
        state.accept( new CountsAccessor.Initializer( target.state ) );
    }

    public <Tracker extends CountsVisitor.Visitable & CountsAccessor> void verify( final Tracker tracker )
    {
        CountsDelta seenState = new CountsDelta();
        final CountsAccessor.Initializer initializer = new CountsAccessor.Initializer( seenState );
        List<CountsDelta.Difference> differences = state.verify(
                verifier -> tracker.accept( CountsVisitor.Adapter.multiplex( initializer, verifier ) ) );
        seenState.accept( new CountsVisitor()
        {
            @Override
            public void visitNodeCount( int labelId, long count )
            {
                long expected = tracker.nodeCount( labelId, newDoubleLongRegister() ).readSecond();
                assertEquals( "Should be able to read visited state.", expected, count );
            }

            @Override
            public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
            {
                long expected = tracker.relationshipCount(
                        startLabelId, typeId, endLabelId, newDoubleLongRegister() ).readSecond();
                assertEquals( "Should be able to read visited state.", expected, count );
            }
        } );
        if ( !differences.isEmpty() )
        {
            StringBuilder errors = new StringBuilder()
                    .append( "Counts differ in " ).append( differences.size() ).append( " places..." );
            for ( CountsDelta.Difference difference : differences )
            {
                errors.append( "\n\t" ).append( difference );
            }
            throw new AssertionError( errors.toString() );
        }
    }
}
