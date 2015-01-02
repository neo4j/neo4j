/**
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
package org.neo4j.kernel.impl.store;

import java.util.List;

import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.register.Register.DoubleLongRegister;

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

    public void indexUpdatesAndSize( int labelId, int propertyKeyId, long updates, long size )
    {
        state.replaceIndexUpdateAndSize( labelId, propertyKeyId, updates, size );
    }

    public void indexSampling( int labelId, int propertyKeyId, long unique, long size )
    {
        state.replaceIndexSample( labelId, propertyKeyId, unique, size );
    }

    public void update( CountsAccessor target )
    {
        state.accept( new CountsAccessor.Initializer( target ) );
    }

    public void update( CountsOracle target )
    {
        update( target.state );
    }

    public void verify( final CountsTracker tracker )
    {
        List<CountsRecordState.Difference> differences = state.verify( new CountsVisitor.Visitable()
        {
            @Override
            public void accept( final CountsVisitor verifier )
            {
                tracker.accept( new CountsVisitor()
                {
                    @Override
                    public void visitNodeCount( int labelId, long count )
                    {
                        long expected = tracker.nodeCount( labelId, newDoubleLongRegister() ).readSecond();
                        assertEquals( "Should be able to read visited state.", expected, count );
                        verifier.visitNodeCount( labelId, count );
                    }

                    @Override
                    public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
                    {
                        long expected = tracker.relationshipCount(
                                startLabelId, typeId, endLabelId, newDoubleLongRegister() ).readSecond();
                        assertEquals( "Should be able to read visited state.", expected, count );
                        verifier.visitRelationshipCount( startLabelId, typeId, endLabelId, count );
                    }

                    @Override
                    public void visitIndexCounts( int labelId, int propertyKeyId, long updates, long size )
                    {
                        DoubleLongRegister output =
                            tracker.indexUpdatesAndSize( labelId, propertyKeyId, newDoubleLongRegister() );
                        assertEquals( "Should be able to read visited state.", output.readFirst(), updates );
                        assertEquals( "Should be able to read visited state.", output.readSecond() , size );
                        verifier.visitIndexCounts( labelId, propertyKeyId, updates, size );
                    }

                    @Override
                    public void visitIndexSample( int labelId, int propertyKeyId, long unique, long size )
                    {
                        DoubleLongRegister output =
                            tracker.indexSample( labelId, propertyKeyId, newDoubleLongRegister() );
                        assertEquals( "Should be able to read visited state.", output.readFirst(), unique );
                        assertEquals( "Should be able to read visited state.", output.readSecond(), size );
                        verifier.visitIndexSample( labelId, propertyKeyId, unique, size );
                    }
                } );
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
