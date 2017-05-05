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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

public abstract class InputIteratorLegacyWrapper<ENTITY extends InputEntity> implements InputIterator
{
    private final SourceTraceability traceability;
    private final int batchSize;

    public InputIteratorLegacyWrapper( SourceTraceability traceability, int batchSize )
    {
        this.traceability = traceability;
        this.batchSize = batchSize;
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public String sourceDescription()
    {
        return traceability.sourceDescription();
    }

    @Override
    public long lineNumber()
    {
        return traceability.lineNumber();
    }

    @Override
    public long position()
    {
        return traceability.position();
    }

    @Override
    public void receivePanic( Throwable cause )
    {
    }

    @Override
    public boolean next( InputChunk chunk ) throws IOException
    {
        return false;
    }

    public class Nodes extends InputIteratorLegacyWrapper<InputNode>
    {
        private final Iterator<InputNode> nodes;

        public Nodes( SourceTraceability traceability, int batchSize, Iterator<InputNode> nodes )
        {
            super( traceability, batchSize );
            this.nodes = nodes;
        }

        @Override
        public InputChunk newChunk()
        {
            return null;
        }
    }

    private static abstract class InputChunkLegacyWrapper<ENTITY extends InputEntity> implements InputChunk
    {
        private final List<ENTITY> entities;
        private int cursor;

        InputChunkLegacyWrapper( int batchSize )
        {
            this.entities = new ArrayList<>();
        }

        @Override
        public void close() throws IOException
        {
        }

        @Override
        public boolean next( InputEntityVisitor visitor ) throws IOException
        {
            if ( cursor >= entities.size() )
            {
                return false;
            }

            ENTITY entity = entities.get( cursor++ );
            visit( entity, visitor );
            return true;
        }

        protected abstract void visit( ENTITY entity, InputEntityVisitor visitor );
    }
}
