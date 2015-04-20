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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.function.Function;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.WriterFactory;
import org.neo4j.unsafe.impl.batchimport.store.io.IoQueue;

import static org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.SYNCHRONOUS;

/**
 * Common ways to create {@link WriterFactory} instances.
 */
public class WriterFactories
{
    public static Function<Configuration,WriterFactory> parallel()
    {
        return new Function<Configuration, WriterFactory>()
        {
            @Override
            public WriterFactory apply( Configuration configuration )
            {
                return new IoQueue( 1, configuration.maxNumberOfIoProcessors(),
                        configuration.workAheadSize()*10, SYNCHRONOUS );
            }
        };
    }

    public static abstract class SingleThreadedWriterFactory implements WriterFactory
    {
        @Override
        public int numberOfProcessors()
        {
            return 1;
        }

        @Override
        public boolean incrementNumberOfProcessors()
        {
            return false;
        }

        @Override
        public boolean decrementNumberOfProcessors()
        {
            return false;
        }
    }
}
