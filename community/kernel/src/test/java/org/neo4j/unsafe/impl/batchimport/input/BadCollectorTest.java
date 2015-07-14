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
package org.neo4j.unsafe.impl.batchimport.input;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.EphemeralFileSystemRule;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.unsafe.impl.batchimport.input.BadCollectorTest.InputRelationshipBuilder.inputRelationship;

public class BadCollectorTest
{
    public final @Rule EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    
    @Test
    public void shouldCollectBadRelationshipsEvenIfThresholdNeverReached() throws IOException
    {
        // given
        int tolerance = 5;

        BadCollector badCollector = new BadCollector( badOutputFile(), tolerance, BadCollector.COLLECT_ALL );

        // when
        badCollector.collectBadRelationship( inputRelationship().build(), 2 );

        // then
        assertEquals( 1, badCollector.badEntries() );
    }

    @Test
    public void shouldThrowExceptionIfNoToleranceThresholdIsExceeded() throws IOException
    {
        // given
        int tolerance = 0;

        BadCollector badCollector = new BadCollector( badOutputFile(), tolerance, BadCollector.COLLECT_ALL );

        // when
        try
        {
            badCollector.collectBadRelationship( inputRelationship().build(), 2 );
            fail( "Should have thrown an InputException" );
        }
        catch ( InputException ignored )
        {
            // then expect to end up here
        }
    }

    @Test
    public void shouldThrowExceptionIfDuplicateNodeTipsUsOverTheToleranceEdge() throws IOException
    {
        // given
        int tolerance = 1;

        BadCollector badCollector = new BadCollector( badOutputFile(), tolerance, BadCollector.COLLECT_ALL );

        // when
        badCollector.collectBadRelationship( inputRelationship().build(), 2 );
        try
        {
            badCollector.collectDuplicateNode( 1, 1, "group", "source", "otherSource" );
            fail( "Should have thrown an InputException" );
        }
        catch ( InputException ignored )
        {
            // then expect to end up here
        }
    }

    @Test
    public void shouldThrowExceptionIfBadRelationshipsTipsUsOverTheToleranceEdge() throws IOException
    {
        // given
        int tolerance = 1;

        BadCollector badCollector = new BadCollector( badOutputFile(), tolerance, BadCollector.COLLECT_ALL );

        // when
        badCollector.collectDuplicateNode( 1, 1, "group", "source", "otherSource" );
        try
        {
            badCollector.collectBadRelationship( inputRelationship().build(), 2 );
            fail( "Should have thrown an InputException" );
        }
        catch ( InputException ignored )
        {
            // then expect to end up here
        }
    }


    @Test
    public void shouldNotCollectBadRelationshipsIfWeShouldOnlyBeCollectingNodes() throws IOException
    {
        // given
        int tolerance = 1;

        BadCollector badCollector = new BadCollector( badOutputFile(), tolerance, BadCollector.DUPLICATE_NODES );

        // when
        badCollector.collectDuplicateNode( 1, 1, "group", "source", "otherSource" );
        try
        {
            badCollector.collectBadRelationship( inputRelationship().build(), 2 );
        }
        catch ( InputException ignored )
        {
            // then expect to end up here
            assertEquals( 1 /* only duplicate node collected */, badCollector.badEntries() );
        }
    }

    @Test
    public void shouldNotCollectBadNodesIfWeShouldOnlyBeCollectingRelationships() throws IOException
    {
        // given
        int tolerance = 1;

        BadCollector badCollector = new BadCollector( badOutputFile(), tolerance, BadCollector.BAD_RELATIONSHIPS );

        // when
        badCollector.collectBadRelationship( inputRelationship().build(), 2 );
        try
        {
            badCollector.collectDuplicateNode( 1, 1, "group", "source", "otherSource" );
        }
        catch ( InputException ignored )
        {
            // then expect to end up here
            assertEquals( 1 /* only duplicate rel collected */, badCollector.badEntries() );
        }
    }

    @Test
    public void shouldBeAbleToRetrieveDuplicateNodeIds() throws IOException
    {
        // given
        int tolerance = 15;

        BadCollector badCollector = new BadCollector( badOutputFile(), tolerance, BadCollector.COLLECT_ALL );

        // when
        for ( int i = 0; i < 15; i++ )
        {
            badCollector.collectDuplicateNode( i, i, "group", "source" + i, "otherSource" + i );
        }

        // then
        assertEquals( 15, PrimitiveLongCollections.count( badCollector.leftOverDuplicateNodesIds() ) );

        PrimitiveLongSet longs = PrimitiveLongCollections.asSet( badCollector.leftOverDuplicateNodesIds() );
        for ( int i = 0; i < 15; i++ )
        {
            assertTrue( longs.contains( i ) );
        }
    }

    private OutputStream badOutputFile() throws IOException
    {
        File badDataPath = new File( "/tmp/foo2" ).getAbsoluteFile();
        FileSystemAbstraction fileSystem = fs.get();
        File badDataFile = badDataFile( fileSystem, badDataPath );
        return fileSystem.openAsOutputStream( badDataFile, true );
    }

    static class InputRelationshipBuilder
    {
        private String sourceDescription = "foo";
        private int lineNumber = 1;
        private int position = 1;
        private Object[] properties = new Object[]{};
        private long firstPropertyId = -1l;
        private Object startNode = null;
        private Object endNode = null;
        private String friend = "FRIEND";
        private int typeId = 1;

        public static InputRelationshipBuilder inputRelationship()
        {
            return new InputRelationshipBuilder();
        }

        InputRelationship build()
        {
            return new InputRelationship( sourceDescription, lineNumber, position,
                    properties, firstPropertyId, startNode, endNode, friend, typeId );
        }
    }

    private File badDataFile( FileSystemAbstraction fileSystem, File badDataPath ) throws IOException
    {
        fileSystem.mkdir( badDataPath.getParentFile() );
        fileSystem.create( badDataPath );
        return badDataPath;
    }
}
