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
package org.neo4j.internal.batchimport;

import static org.neo4j.io.ByteUnit.bytesToString;

import org.neo4j.internal.batchimport.cache.GatheringMemoryStatsVisitor;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.internal.batchimport.stats.DetailLevel;
import org.neo4j.internal.batchimport.stats.GenericStatsProvider;
import org.neo4j.internal.batchimport.stats.Keys;
import org.neo4j.internal.batchimport.stats.Stat;

/**
 * Provides {@link Stat statistics} about memory usage, as the key {@link Keys#memory_usage}
 */
public class MemoryUsageStatsProvider extends GenericStatsProvider implements Stat {
    private final MemoryStatsVisitor.Visitable[] users;

    public MemoryUsageStatsProvider(MemoryStatsVisitor.Visitable... users) {
        this.users = users;
        add(Keys.memory_usage, this);
    }

    @Override
    public DetailLevel detailLevel() {
        return DetailLevel.IMPORTANT;
    }

    @Override
    public long asLong() {
        GatheringMemoryStatsVisitor visitor = new GatheringMemoryStatsVisitor();
        for (MemoryStatsVisitor.Visitable user : users) {
            user.acceptMemoryStatsVisitor(visitor);
        }
        return visitor.getHeapUsage() + visitor.getOffHeapUsage();
    }

    @Override
    public String toString() {
        return bytesToString(asLong());
    }
}
