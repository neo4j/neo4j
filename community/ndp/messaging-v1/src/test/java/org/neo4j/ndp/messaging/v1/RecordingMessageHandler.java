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
package org.neo4j.ndp.messaging.v1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.ndp.messaging.v1.message.AcknowledgeFailureMessage;
import org.neo4j.ndp.messaging.v1.message.DiscardAllMessage;
import org.neo4j.ndp.messaging.v1.message.FailureMessage;
import org.neo4j.ndp.messaging.v1.message.IgnoredMessage;
import org.neo4j.ndp.messaging.v1.message.InitializeMessage;
import org.neo4j.ndp.messaging.v1.message.Message;
import org.neo4j.ndp.messaging.v1.message.PullAllMessage;
import org.neo4j.ndp.messaging.v1.message.RecordMessage;
import org.neo4j.ndp.messaging.v1.message.RunMessage;
import org.neo4j.ndp.messaging.v1.message.SuccessMessage;
import org.neo4j.ndp.runtime.spi.Record;

public class RecordingMessageHandler implements MessageHandler<RuntimeException>
{
    private List<Message> messages = new ArrayList<>();

    @Override
    public void handleRunMessage( String statement, Map<String,Object> params )
    {
        messages.add( new RunMessage( statement, params ) );
    }

    @Override
    public void handlePullAllMessage()
    {
        messages.add( new PullAllMessage() );
    }

    @Override
    public void handleDiscardAllMessage()
    {
        messages.add( new DiscardAllMessage() );
    }

    @Override
    public void handleAckFailureMessage() throws RuntimeException
    {
        messages.add( new AcknowledgeFailureMessage() );
    }

    @Override
    public void handleRecordMessage( Record item )
    {
        messages.add( new RecordMessage( item ) );
    }

    @Override
    public void handleSuccessMessage( Map<String,Object> metadata )
    {
        messages.add( new SuccessMessage( metadata ) );
    }

    @Override
    public void handleFailureMessage( Status status, String message )
    {
        messages.add( new FailureMessage( status, message ) );
    }

    @Override
    public void handleIgnoredMessage() throws RuntimeException
    {
        messages.add( new IgnoredMessage() );
    }

    @Override
    public void handleInitializeMessage( String clientName ) throws RuntimeException
    {
        messages.add( new InitializeMessage( clientName ) );
    }

    public List<Message> asList()
    {
        return messages;
    }
}
