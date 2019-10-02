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

import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.server.http.cypher.TransactionHandle;

/**
 * A representation of a source of events that are produced by Cypher resource.
 * <p>
 * It also provides information that might be useful to {@link javax.ws.rs.ext.MessageBodyWriter}s, when producing the output format.
 * <p>
 * The following diagram shows where output events and their source fit in the outgoing response processing:
 * <p>
 * (CypherResource) - [OutputEventSource producing OutputEvents] -> (MessageBodyWriter) - [OutputStream] -> (network)
 */
public interface OutputEventSource
{
    /**
     * Producing output events.
     * <p>
     * The events will be produced using the caller thread and the method returns only after last output event has been produced.
     *
     * @param eventListener consumer of the produced events.
     */
    void produceEvents( Consumer<OutputEvent> eventListener );

    /**
     * Gets parameters passed to this object from {@link InputEventStream}. The parameters are useful primarily if an input format
     * contains parameters for an output format. Input and output formats linked in this way can transfer parameters using this map.
     *
     * @return parameters.
     */
    Map<String,Object> getParameters();

    TransactionHandle getTransactionHandle();

    TransactionUriScheme getUriInfo();
}
