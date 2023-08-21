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
package org.neo4j.internal.batchimport.stats;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Generic implementation for providing {@link Stat statistics}.
 */
public class GenericStatsProvider implements StatsProvider {
    private final Collection<KeyStatistics> stats = new ArrayList<>();

    public void add(Key key, Stat stat) {
        this.stats.add(new KeyStatistics(key, stat));
    }

    @Override
    public Stat stat(Key key) {
        for (KeyStatistics stat1 : stats) {
            if (stat1.key().name().equals(key.name())) {
                return stat1.stat();
            }
        }
        return null;
    }

    @Override
    public Key[] keys() {
        Key[] keys = new Key[stats.size()];
        int i = 0;
        for (KeyStatistics stat : stats) {
            keys[i++] = stat.key();
        }
        return keys;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (KeyStatistics stat : stats) {
            builder.append(builder.length() > 0 ? ", " : "")
                    .append(format("%s: %s", stat.key().shortName(), stat.stat()));
        }
        return builder.toString();
    }

    private record KeyStatistics(Key key, Stat stat) {}
}
