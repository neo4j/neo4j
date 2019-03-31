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

/**
 * An event produced by {@link OutputEventSource}.
 * <p>
 * The event stream has the following general structure:
 * <p>
 * <ul>
 *     <li>
 *      <ul>
 *         <li>STATEMENT_START</li>
 *         <li>RECORD</li>
 *         <li>RECORD</li>
 *         <li>RECORD</li>
 *         <li>...</li>
 *         <li>STATEMENT_END</li>
 *      </ul>
 *     </li>
 *     <li>
 *      <ul>
 *         <li>STATEMENT_START</li>
 *         <li>RECORD</li>
 *         <li>...</li>
 *         <li>STATEMENT_END</li>
 *      </ul>
 *     </li>
 *     <li>TRANSACTION_INFO</li>
 * </ul>
 * <p>
 * TRANSACTION_INFO is always a the last event and provided information about the transaction used for processing the request.
 * FAILURE event can appear anywhere in the stream except after TRANSACTION_INFO event. No statements are executed after a failure,
 * so when it appears, it is typically the second last event in the stream followed only by TRANSACTION_INFO.
 * <p>
 * Typically, only up to one failure event should appear in the event stream. The only case when two failure events can appear in the stream is
 * when a rollback caused by the failure also fails. The second failure message carried the reason for the rollback failure in this case.
 * <p>
 * A stream with  a failure has the following general structure:
 * <p>
 * <ul>
 *     <li>
 *      <ul>
 *         <li>STATEMENT_START</li>
 *         <li>RECORD</li>
 *         <li>...</li>
 *         <li>STATEMENT_END</li>
 *      </ul>
 *     </li>
 *     <li>
 *      <ul>
 *         <li>STATEMENT_START</li>
 *         <li>RECORD</li>
 *         <li>...</li>
 *         <li>FAILURE</li>
 *      </ul>
 *     </li>
 *     <li>TRANSACTION_INFO</li>
 * </ul>
 * There is an error while the second statement is executed in the example above.
 */
public interface OutputEvent
{

    Type getType();

    enum Type
    {

        TRANSACTION_INFO,
        STATEMENT_START,
        STATEMENT_END,
        RECORD,
        FAILURE
    }
}
