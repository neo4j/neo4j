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
package org.neo4j.unsafe.impl.batchimport;

import java.util.NoSuchElementException;

import org.neo4j.csv.reader.Readables;
import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.unsafe.impl.batchimport.input.Input;

/**
 * A {@link ResourceIterator} with added methods suitable for {@link Input} into a {@link BatchImporter}.
 */
public interface InputIterator<T> extends ResourceIterator<T>, SourceTraceability
{
    public static class Adapter<T> extends SourceTraceability.Adapter implements InputIterator<T>
    {
        @Override
        public String sourceDescription()
        {
            return Readables.EMPTY.sourceDescription();
        }

        @Override
        public void close()
        {   // Nothing to close
        }

        @Override
        public boolean hasNext()
        {
            return false;
        }

        @Override
        public T next()
        {
            throw new NoSuchElementException();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
