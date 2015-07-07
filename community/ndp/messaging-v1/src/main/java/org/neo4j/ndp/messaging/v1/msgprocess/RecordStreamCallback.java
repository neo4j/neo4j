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
package org.neo4j.ndp.messaging.v1.msgprocess;

import java.io.IOException;

import org.neo4j.logging.Log;
import org.neo4j.ndp.runtime.spi.Record;
import org.neo4j.ndp.runtime.spi.RecordStream;

public class RecordStreamCallback extends MessageProcessingCallback<RecordStream>
{
    public RecordStreamCallback( Log log )
    {
        super( log );
    }

    @Override
    public void result( RecordStream stream, Void ignore ) throws Exception
    {
        stream.accept( new RecordStream.Visitor()
        {
            @Override
            public void visit( Record record ) throws IOException
            {
                out.handleRecordMessage( record );
            }
        } );
    }
}
