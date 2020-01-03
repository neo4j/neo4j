/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.glassfish.jersey.internal.util.Producer;

import java.util.Collections;
import java.util.Map;

/**
 * A representation of a stream of events that are consumed by Cypher resource.
 * <p>
 * Currently, only one event is supported and which represents a request for statement execution.
 * In other words, this is currently only a stream of statements.
 * <p>
 * The following diagram shows where this input stream fits in the incoming request processing:
 * <p>
 * (network) - [InputStream] -> (MessageBodyReader) - [InputEventStream] -> (CypherResource)
 */
public class InputEventStream
{

    /**
     * An empty input stream used primarily for HTTP requests with no body.
     */
    public static final InputEventStream EMPTY = new InputEventStream( Collections.emptyMap(), () -> null );

    private final Producer<Statement> statementProducer;
    private final Map<String,Object> parameters;

    /**
     * @param parameters parameters that will be passed to {@link OutputEventSource}. The parameters are useful primarily if an input format
     * contains parameters for an output format. Input and output formats linked in this way can transfer parameters using this map.
     * @param statementProducer a function used for obtaining statements. Statements do not need to be fully received from the network
     * when this object is created, so this function serves as a callback for getting the statements.
     * The function should be blocking with {@code null} meaning end of the stream.
     */
    public InputEventStream( Map<String,Object> parameters, Producer<Statement> statementProducer )
    {
        this.parameters = parameters;
        this.statementProducer = statementProducer;
    }

    /**
     * Reads the next statements.
     * <p>
     * The function is blocking with {@code null} meaning end of the stream.
     *
     * @return the next statement or {@code null}.
     */
    public Statement read()
    {
        return statementProducer.call();
    }

    public Map<String,Object> getParameters()
    {
        return parameters;
    }
}
