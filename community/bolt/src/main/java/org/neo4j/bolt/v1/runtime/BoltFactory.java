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
package org.neo4j.bolt.v1.runtime;

import org.neo4j.bolt.BoltChannel;

import java.time.Clock;

/**
 * Factory class for Bolt runtime environments.
 */
public interface BoltFactory
{
    /**
     * Generate a new state machine.
     *
     * @param boltChannel channel over which Bolt massages can be exchanged
     * @param clock used to keep track of execution times
     * @return new {@link BoltStateMachine} instance
     */
    BoltStateMachine newMachine( BoltChannel boltChannel, Clock clock );
}
