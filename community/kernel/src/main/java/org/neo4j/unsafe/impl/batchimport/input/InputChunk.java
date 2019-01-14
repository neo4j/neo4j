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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.Closeable;
import java.io.IOException;

/**
 * A chunk of data which an {@link InputEntityVisitor} can visit to extract data from. There may be zero or
 * more entities in a chunk.
 */
public interface InputChunk extends Closeable
{
    InputChunk EMPTY = new InputChunk()
    {
        @Override
        public boolean next( InputEntityVisitor visitor )
        {
            return false;
        }

        @Override
        public void close()
        {
        }
    };

    boolean next( InputEntityVisitor visitor ) throws IOException;
}
