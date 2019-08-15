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
package org.neo4j.bolt.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.bolt.v3.messaging.response.RecordMessage;
import org.neo4j.values.AnyValue;

public class BoltResponseMessageRecorder implements BoltResponseMessageWriter
{
    private final List<ResponseMessage> messages = new ArrayList<>();
    private AnyValue[] fields;
    private int currentOffset = -1;

    public List<ResponseMessage> asList()
    {
        return messages;
    }

    @Override
    public void write( ResponseMessage message )
    {
        messages.add( message.copy() );
    }

    @Override
    public void flush() throws IOException
    {
        // do nothing
    }

    @Override
    public void beginRecord( int numberOfFields )
    {
        currentOffset = 0;
        fields = new AnyValue[numberOfFields];
    }

    @Override
    public void consumeField( AnyValue value )
    {
        fields[currentOffset++] = value;
    }

    @Override
    public void endRecord()
    {
        currentOffset = -1;
        messages.add( new RecordMessage( fields ) );
    }

    @Override
    public void onError()
    {
        //IGNORE
    }
}
