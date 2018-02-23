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
package org.neo4j.kernel.impl.store;

import java.util.List;

import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.register.Register;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private final CountsRecordState state = new CountsRecordState();

    public Node node( long... labels )
    {
        state.addNode( labels );
        return new Node( labels );
    }

    public void relationship( Node start, int type, Node end )
    {
        state.addRelationship( start.labels, type, end.labels );
    }

    public void indexUpdatesAndSize( long indexId, long updates, long size )
    {
        state.replaceIndexUpdateAndSize( indexId, updates, size );
    }

    public void indexSampling( long indexId, long unique, long size )
    {
        state.replaceIndexSample( indexId, unique, size );
    }

    public void update( CountsTracker target, long txId )
    {
        try ( CountsAccessor.Updater updater = target.apply( txId ).get();
              CountsAccessor.IndexStatsUpdater stats = target.updateIndexCounts() )
        {
            state.accept( new CountsAccessor.Initializer( updater, stats ) );
        }
    }

    public void update( CountsOracle target )
    {
        state.accept( new CountsAccessor.Initializer( target.state, target.state ) );
    }

    public <Tracker extends CountsVisitor.Visitable & CountsAccessor> void verify( final Tracker tracker )
    {
        CountsRecordState seenState = new CountsRecordState();
        final CountsAccessor.Initializer initializer = new CountsAccessor.Initializer( seenState, seenState );
        List<CountsRecordState.Difference> differences = state.verify(
                verifier -> tracker.accept( CountsVisitor.Adapter.multiplex( initializer, verifier ) ) );
        seenState.accept( new CountsVisitor()
        {
            @Override
            public void visitNodeCount( int labelId, long count )
            {
                long expected = tracker.nodeCount( labelId, newDoubleLongRegister() ).readSecond();
                assertEquals( expected, count, "Should be able to read visited state." );
            }

            @Override
            public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
            {
                long expected = tracker.relationshipCount(
                        startLabelId, typeId, endLabelId, newDoubleLongRegister() ).readSecond();
                assertEquals( expected, count, "Should be able to read visited state." );
            }

            @Override
            public void visitIndexStatistics( long indexId, long updates, long size )
            {
                Register.DoubleLongRegister output =
                        tracker.indexUpdatesAndSize( indexId, newDoubleLongRegister() );
                assertEquals( output.readFirst(), updates, "Should be able to read visited state." );
                assertEquals( output.readSecond(), size, "Should be able to read visited state." );
            }

            @Override
            public void visitIndexSample( long indexId, long unique, long size )
            {
                Register.DoubleLongRegister output =
                        tracker.indexSample( indexId, newDoubleLongRegister() );
                assertEquals( output.readFirst(), unique, "Should be able to read visited state." );
                assertEquals( output.readSecond(), size, "Should be able to read visited state." );
            }
        } );
        if ( !differences.isEmpty() )
        {
            StringBuilder errors = new StringBuilder()
                    .append( "Counts differ in " ).append( differences.size() ).append( " places..." );
            for ( CountsRecordState.Difference difference : differences )
            {
                errors.append( "\n\t" ).append( difference );
            }
            throw new AssertionError( errors.toString() );
        }
    }
}
