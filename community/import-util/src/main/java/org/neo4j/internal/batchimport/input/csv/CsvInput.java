/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.batchimport.input.csv;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

import org.neo4j.collection.RawIterator;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.MultiReadable;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.input.Inputs;
import org.neo4j.internal.batchimport.input.PropertySizeCalculator;
import org.neo4j.internal.batchimport.input.ReadableGroups;
import org.neo4j.io.ByteUnit;
import org.neo4j.memory.MemoryTracker;

import static org.apache.commons.lang3.SystemUtils.IS_OS_LINUX;
import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.internal.batchimport.input.Collector.EMPTY;
import static org.neo4j.internal.batchimport.input.csv.CsvInputIterator.extractHeader;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

/**
 * Provides {@link Input} from data contained in tabular/csv form. Expects factories for instantiating
 * the {@link CharSeeker} objects seeking values in the csv data and header factories for how to
 * extract meta data about the values.
 */
public class CsvInput implements Input
{
    private static final long ESTIMATE_SAMPLE_SIZE = mebiBytes( 1 );

    private final Iterable<DataFactory> nodeDataFactory;
    private final Header.Factory nodeHeaderFactory;
    private final Iterable<DataFactory> relationshipDataFactory;
    private final Header.Factory relationshipHeaderFactory;
    private final IdType idType;
    private final Configuration config;
    private final Monitor monitor;
    private final Groups groups;
    private final MemoryTracker memoryTracker;

    /**
     * @param nodeDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code nodeHeaderFactory}. From the outside
     * it looks like one stream of nodes.
     * @param nodeHeaderFactory factory for reading node headers.
     * @param relationshipDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code relationshipHeaderFactory}.
     * From the outside it looks like one stream of relationships.
     * @param relationshipHeaderFactory factory for reading relationship headers.
     * @param idType {@link IdType} to expect in id fields of node and relationship input.
     * @param config CSV configuration.
     * @param monitor {@link Monitor} for internal events.
     */
    public CsvInput(
            Iterable<DataFactory> nodeDataFactory, Header.Factory nodeHeaderFactory,
            Iterable<DataFactory> relationshipDataFactory, Header.Factory relationshipHeaderFactory,
            IdType idType, Configuration config, Monitor monitor, MemoryTracker memoryTracker )
    {
        this( nodeDataFactory, nodeHeaderFactory, relationshipDataFactory, relationshipHeaderFactory, idType, config, monitor, new Groups(), memoryTracker );
    }

    CsvInput(
            Iterable<DataFactory> nodeDataFactory, Header.Factory nodeHeaderFactory,
            Iterable<DataFactory> relationshipDataFactory, Header.Factory relationshipHeaderFactory,
            IdType idType, Configuration config, Monitor monitor, Groups groups, MemoryTracker memoryTracker )
    {
        this.memoryTracker = memoryTracker;
        assertSaneConfiguration( config );

        this.nodeDataFactory = nodeDataFactory;
        this.nodeHeaderFactory = nodeHeaderFactory;
        this.relationshipDataFactory = relationshipDataFactory;
        this.relationshipHeaderFactory = relationshipHeaderFactory;
        this.idType = idType;
        this.config = config;
        this.monitor = monitor;
        this.groups = groups;

        verifyHeaders();
        warnAboutDuplicateSourceFiles();
    }

    /**
     * Verifies so that all headers in input files looks sane:
     * <ul>
     * <li>node/relationship headers can be parsed correctly</li>
     * <li>relationship headers uses ID spaces previously defined in node headers</li>
     * </ul>
     */
    private void verifyHeaders()
    {
        try
        {
            // parse all node headers and remember all ID spaces
            for ( DataFactory dataFactory : nodeDataFactory )
            {
                try ( CharSeeker dataStream = charSeeker( new MultiReadable( dataFactory.create( config ).stream() ), config, true ) )
                {
                    // Parsing and constructing this header will create this group,
                    // so no need to do something with the result of it right now
                    nodeHeaderFactory.create( dataStream, config, idType, groups, NO_MONITOR );
                }
            }

            // parse all relationship headers and verify all ID spaces
            for ( DataFactory dataFactory : relationshipDataFactory )
            {
                try ( CharSeeker dataStream = charSeeker( new MultiReadable( dataFactory.create( config ).stream() ), config, true ) )
                {
                    // Merely parsing and constructing the header here will as a side-effect verify that the
                    // id groups already exists (relationship header isn't allowed to create groups)
                    relationshipHeaderFactory.create( dataStream, config, idType, groups, NO_MONITOR );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void warnAboutDuplicateSourceFiles()
    {
        try
        {
            Set<String> seenSourceFiles = new HashSet<>();
            warnAboutDuplicateSourceFiles( seenSourceFiles, nodeDataFactory );
            warnAboutDuplicateSourceFiles( seenSourceFiles, relationshipDataFactory );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void warnAboutDuplicateSourceFiles( Set<String> seenSourceFiles, Iterable<DataFactory> dataFactories ) throws IOException
    {
        for ( DataFactory dataFactory : dataFactories )
        {
            RawIterator<CharReadable,IOException> stream = dataFactory.create( config ).stream();
            while ( stream.hasNext() )
            {
                try ( CharReadable source = stream.next() )
                {
                    warnAboutDuplicateSourceFiles( seenSourceFiles, source );
                }
            }
        }
    }

    private void warnAboutDuplicateSourceFiles( Set<String> seenSourceFiles, CharReadable source )
    {
        String sourceDescription = source.sourceDescription();
        if ( !seenSourceFiles.add( sourceDescription ) )
        {
            monitor.duplicateSourceFile( sourceDescription );
        }
    }

    private static void assertSaneConfiguration( Configuration config )
    {
        Map<Character,String> delimiters = new HashMap<>();
        delimiters.put( config.delimiter(), "delimiter" );
        checkUniqueCharacter( delimiters, config.arrayDelimiter(), "array delimiter" );
        checkUniqueCharacter( delimiters, config.quotationCharacter(), "quotation character" );
    }

    private static void checkUniqueCharacter( Map<Character,String> characters, char character, String characterDescription )
    {
        String conflict = characters.put( character, characterDescription );
        if ( conflict != null )
        {
            throw new IllegalArgumentException( "Character '" + character + "' specified by " + characterDescription +
                    " is the same as specified by " + conflict );
        }
    }

    @Override
    public InputIterable nodes( Collector badCollector )
    {
        return () -> stream( nodeDataFactory, nodeHeaderFactory, badCollector );
    }

    @Override
    public InputIterable relationships( Collector badCollector )
    {
        return () -> stream( relationshipDataFactory, relationshipHeaderFactory, badCollector );
    }

    private InputIterator stream( Iterable<DataFactory> data, Header.Factory headerFactory, Collector badCollector )
    {
        return new CsvGroupInputIterator( data.iterator(), headerFactory, idType, config, badCollector, groups, NO_MONITOR );
    }

    @Override
    public IdType idType()
    {
        return idType;
    }

    @Override
    public ReadableGroups groups()
    {
        return groups;
    }

    @Override
    public Estimates calculateEstimates( PropertySizeCalculator valueSizeCalculator ) throws IOException
    {
        long[] nodeSample = sample( nodeDataFactory, nodeHeaderFactory, valueSizeCalculator, node -> node.labels().length );
        long[] relationshipSample = sample( relationshipDataFactory, relationshipHeaderFactory, valueSizeCalculator, entity -> 0 );
        long propPreAllocAdditional = propertyPreAllocateRounding( nodeSample[2] + relationshipSample[2] ) / 2;
        return Input.knownEstimates(
                nodeSample[0], relationshipSample[0],
                nodeSample[1], relationshipSample[1],
                nodeSample[2] + propPreAllocAdditional, relationshipSample[2] + propPreAllocAdditional,
                nodeSample[3] );
    }

    private long[] sample( Iterable<DataFactory> dataFactories, Header.Factory headerFactory,
            PropertySizeCalculator valueSizeCalculator, ToIntFunction<InputEntity> additionalCalculator ) throws IOException
    {
        long[] estimates = new long[4]; // [entity count, property count, property size, labels (for nodes only)]
        try ( CsvInputChunkProxy chunk = new CsvInputChunkProxy() )
        {
            // One group of input files
            int groupId = 0;
            for ( DataFactory dataFactory : dataFactories ) // one input group
            {
                groupId++;
                Header header = null;
                Data data = dataFactory.create( config );
                RawIterator<CharReadable,IOException> sources = data.stream();
                while ( sources.hasNext() )
                {
                    try ( CharReadable source = sources.next() )
                    {
                        if ( header == null )
                        {
                            // Extract the header from the first file in this group
                            // This is the only place we monitor type normalization because it's before import and it touches all headers
                            header = extractHeader( source, headerFactory, idType, config, groups, monitor );
                        }
                        try ( CsvInputIterator iterator = new CsvInputIterator( source, data.decorator(), header, config,
                                idType, EMPTY, CsvGroupInputIterator.extractors( config ), groupId );
                              InputEntity entity = new InputEntity() )
                        {
                            int entities = 0;
                            int properties = 0;
                            int propertySize = 0;
                            int additional = 0;
                            while ( iterator.position() < ESTIMATE_SAMPLE_SIZE && iterator.next( chunk ) )
                            {
                                for ( ; chunk.next( entity ); entities++ )
                                {
                                    properties += entity.propertyCount();
                                    propertySize += Inputs.calculatePropertySize( entity, valueSizeCalculator, NULL, memoryTracker );
                                    additional += additionalCalculator.applyAsInt( entity );
                                }
                            }
                            if ( entities > 0 )
                            {
                                long entityCountInSource = (long) (((double) source.length() / iterator.position()) * entities);
                                estimates[0] += entityCountInSource;
                                estimates[1] += ((double) properties / entities) * entityCountInSource;
                                estimates[2] += ((double) propertySize / entities) * entityCountInSource;
                                estimates[3] += ((double) additional / entities) * entityCountInSource;
                            }
                        }
                    }
                }
            }
        }
        return estimates;
    }

    private long propertyPreAllocateRounding( long initialEstimatedPropertyStoreSize )
    {
        if ( !IS_OS_LINUX )
        {
            // Only linux systems does pre-allocation of store files, so the pre-allocation rounding is zero for all other systems.
            return 0;
        }
        // By default, the page cache will grow large store files in 32 MiB sized chunks.
        long preAllocSize = ByteUnit.mebiBytes( 32 );
        if ( initialEstimatedPropertyStoreSize < preAllocSize )
        {
            return 0;
        }
        long chunks = 1 + initialEstimatedPropertyStoreSize / preAllocSize;
        long estimatedFinalPropertyStoreSize = chunks * preAllocSize;
        // Compute the difference from the initial estimate, to what we anticipate when we account for pre-allocation.
        return estimatedFinalPropertyStoreSize - initialEstimatedPropertyStoreSize;
    }

    public static Extractor<?> idExtractor( IdType idType, Extractors extractors )
    {
        switch ( idType )
        {
        case STRING:
            return extractors.string();
        case INTEGER:
        case ACTUAL:
            return extractors.long_();
        default:
            throw new IllegalArgumentException( "Unsupported id type " + idType );
        }
    }

    public interface Monitor extends Header.Monitor
    {
        /**
         * Reports that a given source file has been specified more than one time.
         * @param sourceFile source file that is a duplicate.
         */
        void duplicateSourceFile( String sourceFile );
    }

    public static final Monitor NO_MONITOR = new Monitor()
    {
        @Override
        public void duplicateSourceFile( String sourceFile )
        {   // no-op
        }

        @Override
        public void typeNormalized( String sourceDescription, String header, String fromType, String toType )
        {   // no-op
        }
    };

    public static class PrintingMonitor extends Header.PrintingMonitor implements Monitor
    {
        private final PrintStream out;

        public PrintingMonitor( PrintStream out )
        {
            super( out );
            this.out = out;
        }

        @Override
        public void duplicateSourceFile( String sourceFile )
        {
            out.println( String.format( "WARN: source file %s has been specified multiple times, this may result in unwanted duplicates", sourceFile ) );
        }
    }
}
