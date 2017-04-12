/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime;

import java.time.Clock;

/**
 * Factory class for Bolt runtime environments.
 */
public interface BoltFactory
{
    /**
     * Generate a new state machine.
     *
     * @param source textual description of the source for rate limiting
     * @param connectionDescriptor textual description of the connection for logging purposes
     * @param onClose callback to call on shutdown of the state machine
     * @param clock used to keep track of execution times
     * @return new {@link BoltStateMachine} instance
     */
    BoltStateMachine newMachine( String source, String connectionDescriptor, Runnable onClose, Clock clock );
}
