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
package org.neo4j.ndp.messaging.v1.message;

import java.util.Collections;
import java.util.Map;

import org.neo4j.ndp.runtime.spi.Record;

public class Messages
{
    private static final PullAllMessage PULL_ALL = new PullAllMessage();
    private static final DiscardAllMessage DISCARD_ALL = new DiscardAllMessage();
    private static final AcknowledgeFailureMessage ACK_FAILURE = new AcknowledgeFailureMessage();
    private static final SuccessMessage SUCCESS = new SuccessMessage( Collections.EMPTY_MAP );

    public static Message run( String statement )
    {
        return new RunMessage( statement );
    }

    public static Message run( String statement, Map<String,Object> parameters )
    {
        return new RunMessage( statement, parameters );
    }

    public static Message initialize( String clientName )
    {
        return new InitializeMessage( clientName );
    }

    public static Message pullAll()
    {
        return PULL_ALL;
    }

    public static Message discardAll()
    {
        return DISCARD_ALL;
    }

    public static Message ackFailure()
    {
        return ACK_FAILURE;
    }

    public static Message record( Record value )
    {
        return new RecordMessage( value );
    }

    public static Message success( Map<String,Object> metadata )
    {
        if( metadata.size() == 0 )
        {
            return SUCCESS;
        }
        return new SuccessMessage( metadata );
    }
}
