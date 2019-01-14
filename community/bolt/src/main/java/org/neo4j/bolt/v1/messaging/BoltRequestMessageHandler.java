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
package org.neo4j.bolt.v1.messaging;

import java.util.Map;

import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.values.virtual.MapValue;

/**
 * Interface defining simple handler methods for each defined Bolt request message.
 */
public interface BoltRequestMessageHandler
{
    void onInit( String userAgent, Map<String,Object> authToken );

    void onAckFailure();

    void onReset();

    void onRun( String statement, MapValue params );

    void onDiscardAll();

    void onPullAll();

    void onExternalError( Neo4jError error );
}
