/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.messaging;

import org.neo4j.causalclustering.core.state.CoreLife;
import org.neo4j.causalclustering.identity.ClusterId;

/**
 * A {@link Inbound.MessageHandler} that can be started and stopped in {@link CoreLife}.
 * It is required that if this MessageHandler delegates to another MessageHandler to handle messages
 * then the delegate will also have lifecycle methods called
 */
public interface LifecycleMessageHandler<M extends Message> extends Inbound.MessageHandler<M>
{
    void start( ClusterId clusterId ) throws Throwable;

    void stop() throws Throwable;
}
