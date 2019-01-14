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

import org.neo4j.bolt.v1.messaging.message.FailureMessage;
import org.neo4j.bolt.v1.messaging.message.IgnoredMessage;
import org.neo4j.bolt.v1.messaging.message.RecordMessage;
import org.neo4j.bolt.v1.messaging.message.ResponseMessage;
import org.neo4j.bolt.v1.messaging.message.SuccessMessage;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.virtual.MapValue;

public class BoltResponseMessageRecorder extends MessageRecorder<ResponseMessage> implements BoltResponseMessageHandler<RuntimeException>
{
    @Override
    public void onSuccess( MapValue metadata )
    {
        messages.add( new SuccessMessage( metadata ) );
    }

    @Override
    public void onRecord( QueryResult.Record item )
    {
        messages.add( new RecordMessage( item ) );
    }

    @Override
    public void onIgnored() throws RuntimeException
    {
        messages.add( new IgnoredMessage() );
    }

    @Override
    public void onFailure( Status status, String errorMessage )
    {
        messages.add( new FailureMessage( status, errorMessage ) );
    }

}
