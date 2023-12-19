/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cluster.com.message;

/**
 * This is used to process a single message to or from a {@link org.neo4j.cluster.statemachine.StateMachine}.
 * They can be chained internally if needed, so that one processor delegates to one or more other processors.
 */
public interface MessageProcessor
{
    boolean process( Message<? extends MessageType> message );
}
