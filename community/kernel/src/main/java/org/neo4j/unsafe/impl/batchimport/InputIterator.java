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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.csv.reader.Readables;
import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.staging.Panicable;

/**
 * A {@link ResourceIterator} with added methods suitable for {@link Input} into a {@link BatchImporter}.
 */
public interface InputIterator<T> extends ResourceIterator<T>, SourceTraceability, Parallelizable, Panicable
{
    abstract class Adapter<T> extends PrefetchingIterator<T> implements InputIterator<T>
    {
        private final SourceTraceability defaults = new SourceTraceability.Adapter()
        {
            @Override
            public String sourceDescription()
            {
                return Readables.EMPTY.sourceDescription();
            }
        };

        @Override
        public String sourceDescription()
        {
            return defaults.sourceDescription();
        }

        @Override
        public long lineNumber()
        {
            return defaults.lineNumber();
        }

        @Override
        public long position()
        {
            return defaults.position();
        }

        @Override
        public void receivePanic( Throwable cause )
        {
        }

        @Override
        public void close()
        {   // Nothing to close
        }
    }

    class Delegate<T> extends PrefetchingIterator<T> implements InputIterator<T>
    {
        protected final InputIterator<T> actual;

        public Delegate( InputIterator<T> actual )
        {
            this.actual = actual;
        }

        @Override
        public void close()
        {
            actual.close();
        }

        @Override
        protected T fetchNextOrNull()
        {
            return actual.hasNext() ? actual.next() : null;
        }

        @Override
        public String sourceDescription()
        {
            return actual.sourceDescription();
        }

        @Override
        public long lineNumber()
        {
            return actual.lineNumber();
        }

        @Override
        public long position()
        {
            return actual.position();
        }

        @Override
        public int processors( int delta )
        {
            return actual.processors( delta );
        }

        @Override
        public void receivePanic( Throwable cause )
        {
            actual.receivePanic( cause );
        }
    }

    class Empty<T> extends Adapter<T>
    {
        @Override
        protected T fetchNextOrNull()
        {
            return null;
        }
    }
}
