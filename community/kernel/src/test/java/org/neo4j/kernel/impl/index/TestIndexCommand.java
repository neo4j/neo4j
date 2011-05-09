package org.neo4j.kernel.impl.index;

import static java.io.File.createTempFile;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.CommonFactories.defaultLogBufferFactory;
import static org.neo4j.kernel.impl.index.IndexCommand.readCommand;

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
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

public class TestIndexCommand
{
    private static final String indexName1 = "persons";
    private static final String indexName2 = "rels";
    private static final long nodeId1 = 10;
    private static final long nodeId2 = 11;
    private static final long nodeId3 = 12;
    private static final long relId1 = 101;
    private static final String key1 = "name";
    private static final String key2 = "title";
    private static final String key3 = "type";
    private static final String stringValue1 = "Mattias";
    private static final String stringValue2 = "Blabla";
    private static final int intValue = 345;
    private static final Map<String, String> someConfig =
            stringMap( "type", "exact", "provider", "lucene" );
    
    @Test
    public void testWriteThenRead() throws Exception
    {
        List<XaCommand> commands = new ArrayList<XaCommand>();
        IndexDefineCommand definitions = new IndexDefineCommand();
        commands.add( definitions );
        commands.add( definitions.create( indexName1, Node.class, someConfig ) );
        commands.add( definitions.add( indexName1, Node.class, nodeId1, key1, stringValue1 ) );
        commands.add( definitions.add( indexName1, Node.class, nodeId1, key2, stringValue2 ) );
        commands.add( definitions.addRelationship( indexName2, Relationship.class, relId1, key3, intValue, nodeId2, nodeId3 ) );
        commands.add( definitions.remove( indexName1, Node.class, nodeId2, key1, stringValue1 ) );
        commands.add( definitions.delete( indexName2, Relationship.class ) );
        File file = writeCommandsToFile( commands );
        
        List<XaCommand> readCommands = readCommandsFromFile( file ); 
        Iterator<XaCommand> commandIterator = commands.iterator();
        for ( XaCommand readCommand : readCommands )
        {
            assertEquals( commandIterator.next(), readCommand );
        }
        file.delete();
    }

    private List<XaCommand> readCommandsFromFile( File file ) throws IOException
    {
        ReadableByteChannel reader = new FileInputStream( file ).getChannel();
        ByteBuffer buffer = ByteBuffer.allocate( 10000 );
        List<XaCommand> commands = new ArrayList<XaCommand>();
        for ( int i = 0; i < 6; i++ )
        {
            XaCommand command = readCommand( reader, buffer );
            if ( command == null )
            {
                break;
            }
            commands.add( command );
        }
        reader.close();
        return commands;
    }

    private File writeCommandsToFile( List<XaCommand> commands ) throws IOException
    {
        File file = createTempFile( "index", "command" );
        FileChannel fileChannel = new RandomAccessFile( file, "rw" ).getChannel();
        LogBuffer writeBuffer = defaultLogBufferFactory( stringMap() ).create( fileChannel );
        for ( XaCommand command : commands )
        {
            command.writeToFile( writeBuffer );
        }
        writeBuffer.force();
        fileChannel.close();
        return file;
    }
}
