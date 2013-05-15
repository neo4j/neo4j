/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.index;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultLogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.util.FileUtils;

import static java.io.File.createTempFile;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.index.IndexCommand.readCommand;
import static org.neo4j.kernel.impl.util.FileUtils.copyFile;

public class TestIndexCommand
{
    private static final String INDEX_NAME_1 = "persons";
    private static final String INDEX_NAME_2 = "rels";
    private static final long NODE_ID_1 = 10;
    private static final long NODE_ID_2 = 11;
    private static final long NODE_ID_3 = 12;
    private static final long REL_ID_1 = 101;
    private static final String KEY_1 = "name";
    private static final String KEY_2 = "title";
    private static final String KEY_3 = "type";
    private static final String STRING_VALUE_1 = "Mattias";
    private static final String STRING_VALUE_2 = "Blabla";
    private static final int INT_VALUE = 345;
    private static final Map<String, String> SOME_CONFIG = stringMap( "type", "exact", "provider", "lucene" );
    
    @Test
    public void testWriteReadTruncate() throws Exception
    {
        List<XaCommand> commands = createSomeCommands();
        Pair<File, List<Long>> writtenCommands = writeCommandsToFile( commands );
        List<XaCommand> readCommands = readCommandsFromFile( writtenCommands.first() ); 
        
        // Assert that the read commands are equal to the written commands
        Iterator<XaCommand> commandIterator = commands.iterator();
        for ( XaCommand readCommand : readCommands )
        {
            assertEquals( commandIterator.next(), readCommand );
        }
        
        // Assert that even truncated files
        // (where commands are cut off in the middle) can be read
        for ( int i = 0; i < commands.size(); i++ )
        {
            long startPosition = writtenCommands.other().get( i );
            long nextStartPosition = i+1 < commands.size() ?
                    writtenCommands.other().get( i+1 ) : writtenCommands.first().length();
            for ( long p = startPosition; p < nextStartPosition; p++ )
            {
                File copy = copyAndTruncateFile( writtenCommands.first(), p );
                List<XaCommand> readTruncatedCommands = readCommandsFromFile( copy );
                assertEquals( i, readTruncatedCommands.size() );
                FileUtils.deleteFile( copy );
            }
        }
        
        writtenCommands.first().delete();
    }

    private File copyAndTruncateFile( File file, long fileSize ) throws IOException
    {
        File copy = createTempFile( "index", "copy" );
        copyFile( file, copy );
        RandomAccessFile raFile = new RandomAccessFile( copy, "rw" );
        try
        {
            raFile.getChannel().truncate( fileSize );
        }
        finally
        {
            raFile.close();
        }
        return copy;
    }

    private List<XaCommand> createSomeCommands()
    {
        List<XaCommand> commands = new ArrayList<XaCommand>();
        IndexDefineCommand definitions = new IndexDefineCommand();
        commands.add( definitions );
        commands.add( definitions.create( INDEX_NAME_1, Node.class, SOME_CONFIG ) );
        commands.add( definitions.add( INDEX_NAME_1, Node.class, NODE_ID_1, KEY_1, STRING_VALUE_1 ) );
        commands.add( definitions.add( INDEX_NAME_1, Node.class, NODE_ID_1, KEY_2, STRING_VALUE_2 ) );
        commands.add( definitions.addRelationship( INDEX_NAME_2, Relationship.class, REL_ID_1, KEY_3, INT_VALUE, NODE_ID_2, NODE_ID_3 ) );
        commands.add( definitions.remove( INDEX_NAME_1, Node.class, NODE_ID_2, KEY_1, STRING_VALUE_1 ) );
        commands.add( definitions.delete( INDEX_NAME_2, Relationship.class ) );
        return commands;
    }

    private List<XaCommand> readCommandsFromFile( File file ) throws IOException
    {
        FileInputStream in = null;
        ReadableByteChannel reader = null;
        List<XaCommand> commands;
        try
        {
            in = new FileInputStream( file );
            reader = in.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate( 10000 );
            commands = new ArrayList<XaCommand>();
            while ( true )
            {
                XaCommand command = readCommand( reader, buffer );
                if ( command == null )
                {
                    break;
                }
                commands.add( command );
            }
        }
        finally
        {
            if ( in != null )
                in.close();
            if ( reader != null )
                reader.close();
        }
        return commands;
    }

    private Pair<File, List<Long>> writeCommandsToFile( List<XaCommand> commands ) throws IOException
    {
        File file = createTempFile( "index", "command" );
        RandomAccessFile randomAccessFile = new RandomAccessFile( file, "rw" );
        List<Long> startPositions;
        try
        {
            FileChannel fileChannel = randomAccessFile.getChannel();
            LogBuffer writeBuffer = new DefaultLogBufferFactory().create( fileChannel );
            startPositions = new ArrayList<Long>();
            for ( XaCommand command : commands )
            {
                startPositions.add( writeBuffer.getFileChannelPosition() );
                command.writeToFile( writeBuffer );
            }
            writeBuffer.force();
            fileChannel.close();
        }
        finally
        {
            randomAccessFile.close();
        }
        return Pair.of( file, startPositions );
    }
}
