/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.neo4j.helpers.collection.Visitor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

abstract class PruningStrategyTest
{
    Segments segments = mock( Segments.class );
    List<SegmentFile> files;

    ArrayList<SegmentFile> createSegmentFiles( int size )
    {
        ArrayList<SegmentFile> list = new ArrayList<>( size );
        for ( int i = 0; i < size; i++ )
        {
            SegmentFile file = mock( SegmentFile.class );
            when( file.header() ).thenReturn( testSegmentHeader( i ) );
            when( file.size() ).thenReturn( 1L );
            list.add( file );
        }
        return list;
    }

    @Before
    public void mockSegmentVisitor()
    {
        doAnswer( invocation ->
        {
            Visitor<SegmentFile,RuntimeException> visitor = invocation.getArgument( 0 );
            ListIterator<SegmentFile> itr = files.listIterator( files.size() );
            boolean terminate = false;
            while ( itr.hasPrevious() && !terminate )
            {
                terminate = visitor.visit( itr.previous() );
            }
            return null;
        } ).when( segments ).visitBackwards( any() );
    }

    private SegmentHeader testSegmentHeader( long value )
    {
        return new SegmentHeader( -1, -1, value - 1, -1 );
    }
}
