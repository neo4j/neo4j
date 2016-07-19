/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.Iterator;
import java.util.function.Function;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.helpers.collection.NestingIterator;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import static org.neo4j.csv.reader.CharSeekers.charSeeker;

/**
 * Able to deserialize one input group. An input group is a list of one or more input files containing
 * its own header. An import can read multiple input groups. Each group is deserialized by
 * {@link InputEntityDeserializer}.
 */
class InputGroupsDeserializer<ENTITY extends InputEntity>
        extends NestingIterator<ENTITY,DataFactory<ENTITY>>
        implements InputIterator<ENTITY>
{
    private final Header.Factory headerFactory;
    private final Configuration config;
    private final IdType idType;
    private InputIterator<ENTITY> currentInput = new InputIterator.Empty<>();
    private long previousInputsCollectivePositions;
    private int previousInputProcessors = 1;
    private boolean currentInputOpen;
    private final int maxProcessors;
    private final DeserializerFactory<ENTITY> factory;
    private final Class<ENTITY> entityClass;

    @FunctionalInterface
    public interface DeserializerFactory<ENTITY extends InputEntity>
    {
        InputEntityDeserializer<ENTITY> create( CharSeeker dataStream, Header dataHeader,
                Function<ENTITY,ENTITY> decorator );
    }

    InputGroupsDeserializer( Iterator<DataFactory<ENTITY>> dataFactory, Header.Factory headerFactory,
            Configuration config, IdType idType, int maxProcessors, DeserializerFactory<ENTITY> factory,
            Class<ENTITY> entityClass )
    {
        super( dataFactory );
        this.headerFactory = headerFactory;
        this.config = config;
        this.idType = idType;
        this.maxProcessors = maxProcessors;
        this.factory = factory;
        this.entityClass = entityClass;
    }

    @Override
    protected InputIterator<ENTITY> createNestedIterator( DataFactory<ENTITY> dataFactory )
    {
        closeCurrent();

        // Open the data stream. It's closed by the batch importer when execution is done.
        Data<ENTITY> data = dataFactory.create( config );
        if ( config.multilineFields() )
        {
            // Use a single-threaded reading and parsing because if we can expect multi-line fields it's
            // nearly impossible to deduce where one row ends and another starts when diving into
            // an arbitrary position in the file.

            CharSeeker dataStream = charSeeker( data.stream(), config, true );

            // Read the header, given the data stream. This allows the header factory to be able to
            // parse the header from the data stream directly. Or it can decide to grab the header
            // from somewhere else, it's up to that factory.
            Header dataHeader = headerFactory.create( dataStream, config, idType );

            InputEntityDeserializer<ENTITY> input = factory.create( dataStream, dataHeader, data.decorator() );
            // It's important that we assign currentInput before calling initialize(), so that if something
            // goes wrong in initialize() and our close() is called we close it properly.
            currentInput = input;
            currentInputOpen = true;
            input.initialize();
        }
        else
        {
            // If the input fields aren't expected to contain multi-line fields we can do an optimization
            // where we have one reader, reading chunks of data, handing over them to one or more parsing
            // threads. The reader will read from its current position and N bytes ahead. When it gets there
            // it will search backwards for the first new-line character and set the chunk end position
            // to that position, effectively un-reading those characters back. This way each chunk will have
            // complete rows of data and can be parsed individually by multiple threads.

            currentInput = new ParallelInputEntityDeserializer<>( data, headerFactory, config, idType,
                    maxProcessors, factory, entityClass  );
            currentInput.processors( previousInputProcessors );
            currentInputOpen = true;
        }

        return currentInput;
    }

    private void closeCurrent()
    {
        if ( currentInputOpen )
        {
            previousInputsCollectivePositions += currentInput.position();
            previousInputProcessors = currentInput.processors( 0 );
            currentInput.close();
            currentInputOpen = false;
        }
    }

    @Override
    public void close()
    {
        closeCurrent();
    }

    @Override
    public long position()
    {
        return previousInputsCollectivePositions + currentInput.position();
    }

    @Override
    public String sourceDescription()
    {
        return currentInput.sourceDescription();
    }

    @Override
    public long lineNumber()
    {
        return currentInput.lineNumber();
    }

    @Override
    public int processors( int delta )
    {
        return currentInput.processors( delta );
    }
}
