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
package org.neo4j.tooling;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.Random;

import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.function.Function;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Deserialization;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.input.csv.Type;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.progress.ProgressMonitorFactory.textual;

/**
 * Utility for generating a nodes.csv and relationships.csv, with random data structured according
 * to supplied headers. Mostly for testing and trying out the batch importer tool.
 */
public class CsvDataGenerator<NODEFORMAT,RELFORMAT>
{
    private final long nodesSeed, relationshipsSeed;
    private final Header nodeHeader;
    private final Header relationshipHeader;
    private final Configuration config;
    private final long nodes;
    private final long relationships;
    private final Function<SourceTraceability,Deserialization<NODEFORMAT>> nodeDeserialization;
    private final Function<SourceTraceability,Deserialization<RELFORMAT>> relDeserialization;
    private final int numberOfLabels;
    private final int numberOfRelationshipTypes;

    public CsvDataGenerator( Header nodeHeader, Header relationshipHeader, Configuration config,
            long nodes, long relationships,
            Function<SourceTraceability,Deserialization<NODEFORMAT>> nodeDeserialization,
            Function<SourceTraceability,Deserialization<RELFORMAT>> relDeserialization,
            int numberOfLabels, int numberOfRelationshipTypes )
    {
        this.nodeHeader = nodeHeader;
        this.relationshipHeader = relationshipHeader;
        this.config = config;
        this.nodes = nodes;
        this.relationships = relationships;
        this.nodeDeserialization = nodeDeserialization;
        this.relDeserialization = relDeserialization;
        this.numberOfLabels = numberOfLabels;
        this.numberOfRelationshipTypes = numberOfRelationshipTypes;
        this.nodesSeed = currentTimeMillis();
        this.relationshipsSeed = nodesSeed+1;
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

    public InputIterator<NODEFORMAT> nodeData()
    {
        return new RandomDataIterator<>( nodeHeader, nodes, new Random( nodesSeed ), nodeDeserialization, nodes,
                numberOfLabels, numberOfRelationshipTypes );
    }

    public InputIterator<RELFORMAT> relationshipData()
    {
        return new RandomDataIterator<>( relationshipHeader, relationships, new Random( relationshipsSeed ),
                relDeserialization, nodes, numberOfLabels, numberOfRelationshipTypes );
    }

    public static void main( String[] arguments ) throws IOException
    {
        Args args = Args.parse( arguments );
        int nodeCount = args.getNumber( "nodes", null ).intValue();
        int relationshipCount = args.getNumber( "relationships", null ).intValue();
        int labelCount = args.getNumber( "labels", 4 ).intValue();
        int relationshipTypeCount = args.getNumber( "relationship-types", 4 ).intValue();

        Configuration config = Configuration.COMMAS;
        Extractors extractors = new Extractors( config.arrayDelimiter() );
        IdType idType = IdType.ACTUAL;
        Header nodeHeader = sillyNodeHeader( idType, extractors );
        Header relationshipHeader = bareboneRelationshipHeader( idType, extractors );

        ProgressListener progress = textual( System.out ).singlePart( "Generating", nodeCount + relationshipCount );
        Function<SourceTraceability,Deserialization<String>> deserialization = StringDeserialization.factory( config );
        CsvDataGenerator<String,String> generator = new CsvDataGenerator<>(
                nodeHeader, relationshipHeader,
                config, nodeCount, relationshipCount,
                deserialization, deserialization,
                labelCount, relationshipTypeCount );
        writeData( generator.serializeNodeHeader(), generator.nodeData(),
                new File( "target", "nodes.csv" ), progress );
        writeData( generator.serializeRelationshipHeader(), generator.relationshipData(),
                new File( "target", "relationships.csv" ), progress );
        progress.done();
    }

    public static Header sillyNodeHeader( IdType idType, Extractors extractors )
    {
        return new Header( new Entry[] {
                new Entry( null, Type.ID, null, idType.extractor( extractors ) ),
                new Entry( "name", Type.PROPERTY, null, extractors.string() ),
                new Entry( "age", Type.PROPERTY, null, extractors.int_() ),
                new Entry( "something", Type.PROPERTY, null, extractors.string() ),
                new Entry( null, Type.LABEL, null, extractors.stringArray() ),
        } );
    }

    public static Header bareboneNodeHeader( IdType idType, Extractors extractors )
    {
        return new Header( new Entry[] {
                new Entry( null, Type.ID, null, idType.extractor( extractors ) ),
                new Entry( null, Type.LABEL, null, extractors.stringArray() ),
        } );
    }

    public static Header bareboneRelationshipHeader( IdType idType, Extractors extractors )
    {
        return new Header( new Entry[] {
                new Entry( null, Type.START_ID, null, idType.extractor( extractors ) ),
                new Entry( null, Type.END_ID, null, idType.extractor( extractors ) ),
                new Entry( null, Type.TYPE, null, extractors.string() )
        } );
    }

    private static void writeData( String header, Iterator<String> iterator, File file,
            ProgressListener progress ) throws IOException
    {
        System.out.println( "Writing " + file.getAbsolutePath() );
        try ( Writer out = new BufferedWriter( new FileWriter( file ), 102*1024*10 ) )
        {
            out.write( header );
            out.append( '\n' );
            while ( iterator.hasNext() )
            {
                out.write( iterator.next() );
                out.append( '\n' );
                progress.add( 1 );
            }
        }
    }
}
