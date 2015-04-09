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
package org.neo4j.driver.internal.spi;

import java.util.Map;

import org.neo4j.driver.Value;

/**
 * A connection is an abstraction provided by an underlying transport implementation,
 * it is the medium that a session is conducted over.
 */
public interface Connection extends AutoCloseable
{
    /**
     * Queue up a run action. The collector will get called with metadata about the stream that will become available
     * for retrieval.
     */
    void run( String statement, Map<String,Value> parameters, StreamCollector collector );

    /**
     * Queue a discard all action, consuming any items left in the current stream.This will
     * close the stream once its completed, allowing another {@link #run(String, java.util.Map, StreamCollector) run}
     */
    void discardAll();

    /**
     * Queue a pull-all action, output will be handed to the collector once the pull starts. This will
     * close the stream once its completed, allowing another {@link #run(String, java.util.Map, StreamCollector) run}
     */
    void pullAll( StreamCollector collector );

    /**
     * Ensure all outstanding actions are carried out on the server.
     */
    void sync();

    @Override
    void close();
}
