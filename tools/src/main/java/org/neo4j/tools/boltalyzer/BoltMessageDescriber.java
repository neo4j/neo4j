/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tools.boltalyzer;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.v1.messaging.MessageHandler;
import org.neo4j.bolt.v1.runtime.spi.Record;
import org.neo4j.kernel.api.exceptions.Status;

public class BoltMessageDescriber implements MessageHandler<RuntimeException>
{
    private final List<String> messages = new ArrayList<>();

    @Override
    public void handleRunMessage( String statement, Map<String,Object> params ) throws RuntimeException
    {
        messages.add( "RUN " + statement );
    }

    @Override
    public void handlePullAllMessage() throws RuntimeException
    {
        messages.add( "PULL_ALL" );
    }

    @Override
    public void handleDiscardAllMessage() throws RuntimeException
    {
        messages.add( "PULL_ALL" );
    }

    @Override
    public void handleAckFailureMessage() throws RuntimeException
    {
        messages.add( "ACK_FAILURE" );
    }

    @Override
    public void handleRecordMessage( Record item ) throws RuntimeException
    {
        messages.add( "RECORD" );
    }

    @Override
    public void handleSuccessMessage( Map<String,Object> metadata ) throws RuntimeException
    {
        messages.add( "SUCCESS" );
    }

    @Override
    public void handleFailureMessage( Status status, String message ) throws RuntimeException
    {
        messages.add( "FAILURE" );
    }

    @Override
    public void handleIgnoredMessage() throws RuntimeException
    {
        messages.add( "IGNORED" );
    }

    @Override
    public void handleInitMessage( String clientName ) throws RuntimeException
    {
        messages.add( "INIT" );
    }

    /** Return a description of all messages recieved since last time this method was called */
    public String flushDescription()
    {
        StringBuilder sb = new StringBuilder();
        for ( String message : messages )
        {
            sb.append( "\n  ").append( message );
        }

        messages.clear();
        return sb.toString();
    }
}