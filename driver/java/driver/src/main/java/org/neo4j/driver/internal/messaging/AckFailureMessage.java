/**
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
package org.neo4j.driver.internal.messaging;

import java.io.IOException;

import static java.lang.String.format;

/**
 * ACK_FAILURE request message
 * <p>
 * Sent by clients to acknowledge receipt of failures sent by the server. This is required to
 * allow optimistic sending of multiple messages before responses have been received - pipelining.
 * <p>
 * When something goes wrong, we want the server to stop processing our already sent messages,
 * but the server cannot tell the difference between what was sent before and after we saw the
 * error.
 * <p>
 * This message acts as a barrier after an error, informing the server that we've seen the error
 * message, and that messages that follow this one are safe to execute.
 */
public class AckFailureMessage implements Message
{
    @Override
    public void dispatch( MessageHandler handler ) throws IOException
    {
        handler.handleAckFailureMessage();
    }

    @Override
    public String toString()
    {
        return format( "[ACK_FAILURE]" );
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null && obj.getClass() == getClass();
    }

    @Override
    public int hashCode()
    {
        return 1;
    }
}
