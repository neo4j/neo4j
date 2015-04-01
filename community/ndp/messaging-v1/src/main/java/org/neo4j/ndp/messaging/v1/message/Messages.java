/**
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

import java.util.Map;

import org.neo4j.stream.Record;

public class Messages
{
    public static Message run( String statement )
    {
        return new RunMessage( statement );
    }

    public static Message run( String statement, Map<String, Object> parameters )
    {
        return new RunMessage( statement, parameters );
    }

    public static Message pullAll()
    {
        return new PullAllMessage();
    }

    public static Message discardAll()
    {
        return new DiscardAllMessage();
    }

    public static Message ackF()
    {
        return new AcknowledgeFailureMessage();
    }

    public static Message record( Record value )
    {
        return new RecordMessage( value );
    }

    public static Message success( Map<String, Object> metadata )
    {
        return new SuccessMessage( metadata );
    }
}
