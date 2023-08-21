/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.index;

/**
 * Used to hold usage statistics of an index.
 *
 * @param lastRead {@link System#currentTimeMillis()} of last query made to the index
 * @param readCount Number of read queries since {@link #trackedSince}
 * @param trackedSince {@link System#currentTimeMillis()} when tracking of usage in index started
 */
public record IndexUsageStats(long lastRead, long readCount, long trackedSince) {}
