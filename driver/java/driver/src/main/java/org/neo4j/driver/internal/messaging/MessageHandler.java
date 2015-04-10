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
import java.util.Map;

import org.neo4j.driver.Value;

public interface MessageHandler
{
    // Requests
    void handleRunMessage( String statement, Map<String,Value> parameters ) throws IOException;

    void handlePullAllMessage() throws IOException;

    void handleDiscardAllMessage() throws IOException;

    void handleAckFailureMessage() throws IOException;

    // Responses
    void handleSuccessMessage( Map<String,Value> meta ) throws IOException;

    void handleRecordMessage( Value[] fields ) throws IOException;

    void handleFailureMessage( String code, String message ) throws IOException;

    void handleIgnoredMessage() throws IOException;
}
