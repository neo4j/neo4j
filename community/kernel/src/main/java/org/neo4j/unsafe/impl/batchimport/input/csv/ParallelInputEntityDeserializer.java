/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.csv.reader.BufferedCharSeeker;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.ProcessingSource;
import org.neo4j.csv.reader.Source.Chunk;
import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.kernel.impl.util.collection.ContinuableArrayCursor;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.executor.TaskExecutionPanicException;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.InputGroupsDeserializer.DeserializerFactory;
import org.neo4j.unsafe.impl.batchimport.staging.TicketedProcessing;

import static org.neo4j.csv.reader.Source.singleChunk;
import static org.neo4j.kernel.impl.util.Validators.emptyValidator;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.noDecorator;

/**
 * Deserializes CSV into {@link InputNode} and {@link InputRelationship} and does so by reading characters
 * in a dedicated thread while letting one or more threads parse the data. This can only safely be used if
 * {@link Configuration#multilineFields()} is {@code false}. Initially only one parsing thread is assigned,
 * more can be assigned at any point in time using {@link #processors(int)}.
 *
 * This class accepts {@link DeserializerFactory}, which normally instantiates {@link InputEntityDeserializer}
 * instances.
 *
 * @param <ENTITY> type of {@link InputEntity} to deserialize into
 */
public class ParallelInputEntityDeserializer<ENTITY extends InputEntity> extends InputIterator.Adapter<ENTITY>
{
    private final ProcessingSource source;
    private final TicketedProcessing<CharSeeker,Header,ENTITY[]> processing;
    private final ContinuableArrayCursor<ENTITY> cursor;
    private SourceTraceability last = SourceTraceability.EMPTY;
    private Decorator<ENTITY> decorator;

    @SuppressWarnings( "unchecked" )
    public ParallelInputEntityDeserializer( Data<ENTITY> data, Header.Factory headerFactory, Configuration config,
            IdType idType, int maxProcessors, int initialProcessors, DeserializerFactory<ENTITY> factory,
            Validator<ENTITY> validator, Class<ENTITY> entityClass )
    {
        // Reader of chunks, characters aligning to nearest newline
        source = new ProcessingSource( data.stream(), config.bufferSize(), maxProcessors );
        try
        {
            // Read first chunk explicitly here since it contains the header
            Chunk firstChunk = source.nextChunk();
            if ( firstChunk.length() == 0 )
            {
                throw new InputException( "No header defined" );
            }
            CharSeeker firstSeeker = new BufferedCharSeeker( singleChunk( firstChunk ), config );
            Header dataHeader = headerFactory.create( firstSeeker, config, idType );

            // Initialize the processing logic for parsing the data in the first chunk, as well as in all other chunks
            decorator = data.decorator();

            // Check if each individual processor can decorate-and-validate themselves or we have to
            // defer that to the batch supplier below. We have to defer if decorator is mutable.
            boolean deferredValidation = decorator.isMutable();
            Decorator<ENTITY> batchDecorator = deferredValidation ? noDecorator() : decorator;
            Validator<ENTITY> batchValidator = deferredValidation ? emptyValidator() : validator;
            processing = new TicketedProcessing<>( "Parallel input parser", maxProcessors, (seeker, header) ->
            {
                // Create a local deserializer for this chunk with NO decoration/validation,
                // this will happen in an orderly fashion in our post-processor below and done like this
                // to cater for decorators which may be mutable and sensitive to ordering, while still putting
                // the work of decorating and validating on the processing threads as to not affect performance.
                InputEntityDeserializer<ENTITY> chunkDeserializer =
                        factory.create( header, seeker, batchDecorator, batchValidator );
                chunkDeserializer.initialize();
                List<ENTITY> entities = new ArrayList<>();
                while ( chunkDeserializer.hasNext() )
                {
                    ENTITY next = chunkDeserializer.next();
                    entities.add( next );
                }
                return entities.toArray( (ENTITY[]) Array.newInstance( entityClass, entities.size() ) );
            },
            () -> dataHeader.clone() /*We need to clone the stateful header to each processing thread*/ );
            processing.processors( initialProcessors - processing.processors( 0 ) );

            // Utility cursor which takes care of moving over processed results from chunk to chunk
            Supplier<ENTITY[]> batchSupplier = rebaseBatches( processing );
            batchSupplier = deferredValidation ?
                    decorateAndValidate( batchSupplier, decorator, validator ) : batchSupplier;
            cursor = new ContinuableArrayCursor<>( batchSupplier );

            // Start an asynchronous slurp of the chunks fed directly into the processors
            processing.slurp( seekers( firstSeeker, source, config ), true );
        }
        catch ( IOException e )
        {
            throw new InputException( "Couldn't read first chunk from input", e );
        }
    }

    private Supplier<ENTITY[]> decorateAndValidate( Supplier<ENTITY[]> actual,
            Decorator<ENTITY> decorator, Validator<ENTITY> validator )
    {
        return () ->
        {
            ENTITY[] entities = actual.get();
            if ( entities != null )
            {
                for ( int i = 0; i < entities.length; i++ )
                {
                    ENTITY entity = decorator.apply( entities[i] );
                    validator.validate( entity );
                    entities[i] = entity;
                }
            }
            return entities;
        };
    }

    @Override
    protected ENTITY fetchNextOrNull()
    {
        boolean hasNext;
        try
        {
            hasNext = cursor.next();
        }
        catch ( TaskExecutionPanicException e )
        {
            // Getting this exception here means that a processor got an exception and put
            // the executor in panic mode. The user would like to see the actual exception
            // so we're going to do a little thing here where we take the cause of this
            // IllegalStateException and throw it, since this ISE is just a wrapper.
            throw Exceptions.launderedException( e.getCause() );
        }

        if ( hasNext )
        {
            ENTITY next = cursor.get();
            // We keep a reference to the last fetched so that the methods from SourceTraceability can
            // be implemented and executed correctly.
            last = next;
            return next;
        }
        return null;
    }

    private static <ENTITY extends InputEntity> Supplier<ENTITY[]> rebaseBatches(
            TicketedProcessing<CharSeeker,Header,ENTITY[]> processing )
    {
        return new Supplier<ENTITY[]>()
        {
            private String currentSourceDescription;
            private long baseLineNumber;
            private long basePosition;

            @Override
            public ENTITY[] get()
            {
                ENTITY[] batch = processing.next();
                if ( batch != null && batch.length > 0 )
                {
                    // OK so we got the next batch from an arbitrary processor (other thread).
                    // It creates the entities with batch-local line number and position because that's all it knows.
                    // We, however, know about all the batches and the order of them so we convert the local
                    // source traceability numbers to global. This will change some fields in the entities
                    // and for thread-visibility it's OK since this thread which executes right here is the one
                    // which gets the batches from this deserializer in the end.

                    // Reset the base numbers if we're venturing into a new source. We rely on the fact that
                    // the ProcessingSource spawning the chunks which have been processed into entities
                    // don't mix entities from different sources in the same batch.
                    ENTITY lastEntity = batch[batch.length - 1];
                    if ( currentSourceDescription == null ||
                            !currentSourceDescription.equals( lastEntity.sourceDescription() ) )
                    {
                        currentSourceDescription = lastEntity.sourceDescription();
                        baseLineNumber = basePosition = 0;
                        currentSourceDescription = lastEntity.sourceDescription();
                    }

                    // Now we rebase the entities on top of the previous batch we've seen
                    for ( ENTITY entity : batch )
                    {
                        entity.rebase( baseLineNumber, basePosition );
                    }

                    // Remember the new numbers to rebase forthcoming batches on
                    if ( lastEntity.sourceDescription().equals( currentSourceDescription ) )
                    {
                        baseLineNumber = lastEntity.lineNumber();
                        basePosition = lastEntity.position();
                    }
                }
                return batch;
            }
        };
    }

    private static Iterator<CharSeeker> seekers( CharSeeker firstSeeker, ProcessingSource source, Configuration config )
    {
        return new PrefetchingIterator<CharSeeker>()
        {
            private boolean firstReturned;

            @Override
            protected CharSeeker fetchNextOrNull()
            {
                // We have the first here explicitly since we read it before starting the general processing
                // and extract the header. We want to read the data in it as well and that's why we get it here
                if ( !firstReturned )
                {
                    firstReturned = true;
                    return firstSeeker;
                }

                // Continue read the next chunk from the source file(s)
                try
                {
                    Chunk chunk = source.nextChunk();
                    return chunk.length() > 0 ? new BufferedCharSeeker( singleChunk( chunk ), config ) : null;
                }
                catch ( IOException e )
                {
                    throw new InputException( "Couldn't get chunk from source", e );
                }
            }
        };
    }

    @Override
    public void close()
    {
        processing.close();
        try
        {
            decorator.close();
            source.close();
        }
        catch ( IOException e )
        {
            throw new InputException( "Couldn't close source of data chunks", e );
        }
        finally
        {
            super.close();
        }
    }

    @Override
    public int processors( int delta )
    {
        return processing.processors( delta );
    }

    @Override
    public String sourceDescription()
    {
        return last.sourceDescription();
    }

    @Override
    public long lineNumber()
    {
        return last.lineNumber();
    }

    @Override
    public long position()
    {
        return last.position();
    }

    @Override
    public void receivePanic( Throwable cause )
    {
        processing.receivePanic( cause );
    }
}
