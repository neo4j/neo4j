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
package org.neo4j.bolt.v1.messaging;

import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.v1.messaging.response.RecordMessage;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.logging.Log;
import org.neo4j.values.AnyValue;

public class ResultHandler extends MessageProcessingHandler
{
    public ResultHandler( BoltResponseMessageWriter handler, BoltConnection connection, Log log )
    {
        super( handler, connection, log );
    }

    @Override
    public void onRecords( final BoltResult result, final boolean pull ) throws Exception
    {
        result.accept( new BoltResult.Visitor()
        {
            @Override
            public void visit( QueryResult.Record record ) throws Exception
            {
                if ( pull )
                {
                    messageWriter.write( new RecordMessage( record ) );
                }
            }

            @Override
            public void addMetadata( String key, AnyValue value )
            {
                onMetadata( key, value );
            }
        } );
    }
}
