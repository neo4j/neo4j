/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.index.IndexCommand.readCommand;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.transaction.xaframework.CommandSerializer;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogChannel;
import org.neo4j.test.TargetDirectory;


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

    private final InMemoryLogChannel channel = new InMemoryLogChannel();
    
    @Rule
    public TargetDirectory.TestDirectory directory = TargetDirectory.forTest( TestIndexCommand.class ).testDirectory();

    @Test
    public void testWriteReadTruncate() throws Exception
    {
        List<Command> commands = createSomeCommands();
        List<Long> writtenCommands = writeCommandsToFile( commands );
        List<Command> readCommands = readCommandsFromChannel(); 
        
        // Assert that the read commands are equal to the written commands
        Iterator<Command> commandIterator = commands.iterator();
        for ( Command readCommand : readCommands )
        {
            assertEquals( commandIterator.next(), readCommand );
        }
        
        // Assert that even truncated files
        // (where commands are cut off in the middle) can be read
        for ( int i = 0; i < commands.size(); i++ )
        {
            long startPosition = writtenCommands.get( i );
            long nextStartPosition = i+1 < commands.size() ?
                    writtenCommands.get( i+1 ) : channel.writerPosition();
            for ( long p = startPosition; p < nextStartPosition; p++ )
            {
                channel.truncateTo( (int) p );
                List<Command> readTruncatedCommands = readCommandsFromChannel();
                assertEquals( i, readTruncatedCommands.size() );
            }
        }
    }
    private List<Command> createSomeCommands()
    {
        List<Command> commands = new ArrayList<Command>();
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

    private List<Command> readCommandsFromChannel() throws IOException
    {
        FileInputStream in = null;
        ReadableByteChannel reader = null;
        List<Command> commands;
        try
        {
            commands = new ArrayList<Command>();
            while ( true )
            {
                Command command = readCommand( channel );
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

    private List<Long> writeCommandsToFile( List<Command> commands ) throws IOException
    {
        CommandSerializer writer = new CommandSerializer( channel ); 
        List<Long> startPositions;
        startPositions = new ArrayList<>();
        for ( Command command : commands )
        {
            startPositions.add( (long) channel.writerPosition() );
            command.accept( writer );
        }
        return startPositions;
    }
}
