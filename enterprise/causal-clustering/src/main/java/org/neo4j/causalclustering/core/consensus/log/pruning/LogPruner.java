/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.log.pruning;

import java.io.IOException;

/**
 * Defines the set of operations needed to execute raft log pruning. Each raft log implementation is expected
 * to provide its own LogPruner implementation that knows about the log structure on disk. The SPI of these
 * implementations will be provided with the appropriate configuration options that define the expected pruning
 * characteristics.
 */
public interface LogPruner
{
    void prune() throws IOException;
}
