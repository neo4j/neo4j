/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.io.Closeable;
import java.io.IOException;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;

/**
 * A {@link ResourceIterator} with added methods suitable for {@link Input} into a {@link BatchImporter}.
 */
public interface InputIterator extends Closeable
{
    InputChunk newChunk();

    boolean next( InputChunk chunk ) throws IOException;

    abstract class Adapter implements InputIterator
    {
        @Override
        public void close()
        {   // Nothing to close
        }
    }

    class Delegate implements InputIterator
    {
        protected final InputIterator actual;

        public Delegate( InputIterator actual )
        {
            this.actual = actual;
        }

        @Override
        public void close() throws IOException
        {
            actual.close();
        }

        @Override
        public InputChunk newChunk()
        {
            return actual.newChunk();
        }

        @Override
        public boolean next( InputChunk chunk ) throws IOException
        {
            return actual.next( chunk );
        }
    }

    class Empty extends Adapter
    {
        @Override
        public InputChunk newChunk()
        {
            return InputChunk.EMPTY;
        }

        @Override
        public boolean next( InputChunk chunk )
        {
            return false;
        }
    }
}
