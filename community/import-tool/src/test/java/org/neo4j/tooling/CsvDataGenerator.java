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
package org.neo4j.tooling;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.Random;

import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;
import org.neo4j.unsafe.impl.batchimport.input.csv.Type;

import static org.neo4j.helpers.progress.ProgressMonitorFactory.textual;

/**
 * Utility for generating a nodes.csv and relationships.csv, with random data structured according
 * to supplied headers. Mostly for testing and trying out the batch importer tool.
 */
public class CsvDataGenerator
{
    private final Random random = new Random();
    private int highNodeId;
    private final Header nodeHeader;
    private final Header relationshipHeader;
    private final Configuration config;

    public CsvDataGenerator( Header nodeHeader, Header relationshipHeader, Configuration config )
    {
        this.nodeHeader = nodeHeader;
        this.relationshipHeader = relationshipHeader;
        this.config = config;
    }

    public String serializeNodeHeader()
    {
        return serializeHeader( nodeHeader );
    }

    private String serializeHeader( Header header )
    {
        StringBuilder builder = new StringBuilder();
        for ( Entry entry : header.entries() )
        {
            if ( builder.length() > 0 )
            {
                builder.append( config.delimiter() );
            }
            serializeHeaderEntry( builder, entry );
        }
        return builder.toString();
    }

    private void serializeHeaderEntry( StringBuilder builder, Entry entry )
    {
        String value;
        switch ( entry.type() )
        {
        case PROPERTY:
            value = entry.name() + ":" + entry.extractor().toString();
            break;
        case IGNORE:
            return;
        default:
            value = (entry.name() != null ? entry.name() : "") + ":" + entry.type().name();
            break;
        }
        builder.append( value );
    }

    public String serializeRelationshipHeader()
    {
        return serializeHeader( relationshipHeader );
    }

    public Iterator<String> pullNodeData()
    {
        return pullIterator( nodeHeader );
    }

    private Iterator<String> pullIterator( final Header header )
    {
        return new PrefetchingIterator<String>()
        {
            @Override
            protected String fetchNextOrNull()
            {
                return serializeDataLine( header );
            }
        };
    }

    private String serializeDataLine( Header header )
    {
        StringBuilder builder = new StringBuilder();
        for ( Entry entry : header.entries() )
        {
            if ( builder.length() > 0 )
            {
                builder.append( config.delimiter() );
            }
            serializeDataEntry( builder, entry );
        }
        return builder.toString();
    }

    private void serializeDataEntry( StringBuilder builder, Entry entry )
    {
        switch ( entry.type() )
        {
        case ID:
            builder.append( highNodeId++ );
            break;
        case PROPERTY:
            randomValue( builder, entry.extractor() );
            break;
        case LABEL:
            randomLabels( builder, config.arrayDelimiter() );
            break;
        case START_ID: case END_ID:
            builder.append( random.nextInt( highNodeId ) );
            break;
        case TYPE:
            builder.append( "TYPE_" ).append( random.nextInt( 4 ) );
            break;
        default:
            return;
        }
    }

    private void randomValue( StringBuilder builder, Extractor<?> extractor )
    {
        // TODO crude way of determining value type
        String type = extractor.toString();
        if ( type.equals( "String" ) )
        {
            randomString( builder );
        }
        else if ( type.equals( "long" ) )
        {
            builder.append( random.nextInt( Integer.MAX_VALUE ) );
        }
        else if ( type.equals( "int" ) )
        {
            builder.append( random.nextInt( 20 ) );
        }
        else
        {
            throw new IllegalArgumentException( "" + extractor );
        }
    }

    public Iterator<String> pullRelationshipData()
    {
        return pullIterator( relationshipHeader );
    }

    private void randomLabels( StringBuilder builder, char arrayDelimiter )
    {
        int length = random.nextInt( 3 );
        for ( int i = 0; i < length; i++ )
        {
            if ( i > 0 )
            {
                builder.append( arrayDelimiter );
            }
            builder.append( "LABEL_" ).append( random.nextInt( 4 ) );
        }
    }

    private void randomString( StringBuilder builder )
    {
        int length = random.nextInt( 10 )+5;
        for ( int i = 0; i < length; i++ )
        {
            builder.append( (char) ('a' + random.nextInt( 20 )) );
        }
    }

    public static void main( String[] arguments ) throws IOException
    {
        Args args = Args.parse( arguments );
        int nodeCount = args.getNumber( "nodes", null ).intValue();
        int relationshipCount = args.getNumber( "relationships", null ).intValue();

        Configuration config = Configuration.COMMAS;
        Extractors extractors = new Extractors( config.arrayDelimiter() );
        Header nodeHeader = new Header( new Entry[] {
                new Entry( null, Type.ID, extractors.string() ),
                new Entry( "name", Type.PROPERTY, extractors.string() ),
                new Entry( "age", Type.PROPERTY, extractors.int_() ),
                new Entry( "something", Type.PROPERTY, extractors.string() ),
                new Entry( null, Type.LABEL, extractors.stringArray() ),
        } );
        Header relationshipHeader = new Header( new Entry[] {
                new Entry( null, Type.START_ID, extractors.string() ),
                new Entry( null, Type.END_ID, extractors.string() ),
                new Entry( null, Type.TYPE, extractors.string() )
        } );

        ProgressListener progress = textual( System.out ).singlePart( "Generating", nodeCount + relationshipCount );
        CsvDataGenerator generator = new CsvDataGenerator( nodeHeader, relationshipHeader, config );
        writeData( generator.serializeNodeHeader(), generator.pullNodeData(),
                new File( "target", "nodes.csv" ), progress, nodeCount );
        writeData( generator.serializeRelationshipHeader(), generator.pullRelationshipData(),
                new File( "target", "relationships.csv" ), progress, relationshipCount );
        progress.done();
    }

    private static void writeData( String header, Iterator<String> iterator, File file,
            ProgressListener progress, int count ) throws IOException
    {
        System.out.println( "Writing " + file.getAbsolutePath() );
        try ( Writer out = new BufferedWriter( new FileWriter( file ), 102*1024*10 ) )
        {
            out.write( header );
            out.append( '\n' );
            for ( int i = 0; i < count; i++ )
            {
                out.write( iterator.next() );
                out.append( '\n' );
                progress.add( 1 );
            }
        }
    }
}
