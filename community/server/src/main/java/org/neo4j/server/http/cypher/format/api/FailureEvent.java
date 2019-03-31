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
package org.neo4j.server.http.cypher.format.api;

import org.neo4j.kernel.api.exceptions.Status;

/**
 * An event indicating a failure.
 * <p>
 * This event can appear anywhere in the stream except after {@link TransactionInfoEvent} event. No statements are executed after a failure,
 * so when it appears, it is typically the second last event in the stream followed only by {@link TransactionInfoEvent}.
 * <p>
 * Typically, only up to one failure event should appear in the event stream. The only case when two failure events can appear in the stream is
 * when a rollback caused by the failure also fails. The second failure message carried the reason for the rollback failure in this case.
 */
public class FailureEvent implements OutputEvent
{

    private final Status status;
    private final String message;

    public FailureEvent( Status status, String message )
    {
        this.status = status;
        this.message = message;
    }

    @Override
    public Type getType()
    {
        return Type.FAILURE;
    }

    public Status getStatus()
    {
        return status;
    }

    public String getMessage()
    {
        return message;
    }
}
