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
package org.neo4j.bolt.v1.messaging;

import java.util.Map;

import org.neo4j.bolt.v1.messaging.message.RequestMessage;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.bolt.v1.messaging.message.AckFailureMessage.ackFailure;
import static org.neo4j.bolt.v1.messaging.message.DiscardAllMessage.discardAll;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.ResetMessage.reset;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;

public class BoltRequestMessageRecorder extends MessageRecorder<RequestMessage> implements BoltRequestMessageHandler<RuntimeException>
{
    @Override
    public void onInit( String clientName, Map<String, Object> credentials ) throws RuntimeException
    {
        messages.add( init( clientName, credentials ) );
    }

    @Override
    public void onAckFailure() throws RuntimeException
    {
        messages.add( ackFailure() );
    }

    @Override
    public void onReset() throws RuntimeException
    {
        messages.add( reset() );
    }

    @Override
    public void onRun( String statement, MapValue params )
    {
        messages.add( run( statement, params ) );
    }

    @Override
    public void onDiscardAll()
    {
        messages.add( discardAll() );
    }

    @Override
    public void onPullAll()
    {
        messages.add( pullAll() );
    }

    @Override
    public void onExternalError( Neo4jError error ) throws RuntimeException
    {
        //ignore
    }
}
