/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.neo4j.io.pagecache.IOLimiter;

/**
 * Control-point for the flushing speed of the check-pointer thread.
 */
public interface CheckPointFlushControl
{
    /**
     * Get the {@link IOLimiter} instance for this {@link CheckPointFlushControl}.
     *
     * @return An {@link IOLimiter} that always matches the current desired restriction on check-pointer IO, as
     * configured by this {@link CheckPointFlushControl}. Never {@code null}.
     */
    IOLimiter getIOLimiter();

    /**
     * Begin a temporary rush, where the normal restrictions on check-pointing IO will be ignored, and the
     * check-pointer will go as fast as possible. This is has uses where check-points are insert during time-critical
     * operations. For instance, shutdown, recovery, upgrades, and so on.
     * <p>
     * Rushing will immediately be reflected in the {@link IOLimiter} associated with this
     * {@link CheckPointFlushControl}, and will effectively make them act as if the IO rate is
     * {@link IOLimiter#unlimited() unlimited}.
     *
     * @return An {@link AutoCloseable} {@link Rush} instance, that should be held "open" for the duration of the rush
     * period. Never {@code null}.
     */
    Rush beginTemporaryRush();
}
