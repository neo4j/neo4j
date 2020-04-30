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
package org.neo4j.fabric.stream;

import org.neo4j.cypher.internal.runtime.InputCursor;
import org.neo4j.cypher.internal.runtime.InputDataStream;
import org.neo4j.values.AnyValue;

public class InputDataStreamImpl implements InputDataStream
{
    private final Rx2SyncStream wrappedStream;
    private InputCursor inputCursor;

    public InputDataStreamImpl( Rx2SyncStream wrappedStream )
    {
        this.wrappedStream = wrappedStream;
        inputCursor = new Cursor();
    }

    @Override
    public InputCursor nextInputBatch()
    {
        return inputCursor;
    }

    private class Cursor implements InputCursor
    {

        private Record currentRecord;

        @Override
        public boolean next()
        {
            currentRecord = wrappedStream.readRecord();

            if ( currentRecord != null )
            {
                return true;
            }

            inputCursor = null;
            return false;
        }

        @Override
        public AnyValue value( int offset )
        {
            return currentRecord.getValue( offset );
        }

        @Override
        public void close()
        {
            // TODO what exactly is close on the cursor supposed to do?
        }
    }
}
