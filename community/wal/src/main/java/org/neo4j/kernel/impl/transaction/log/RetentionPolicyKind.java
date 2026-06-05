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
package org.neo4j.kernel.impl.transaction.log;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Shared vocabulary for log retention policies. Parsed from {@code db.tx_log.rotation.retention_policy} and
 * {@code db.cluster.raft.log.prune_strategy} and dispatched by both the kernel {@code LogPruneStrategyFactory}
 * (version-based pruning of standard tx logs) and the wal {@code EnvelopedLogPruneStrategyFactory}
 * (segment-based pruning of enveloped raft / merged logs). Centralised so the two factories cannot drift on
 * which keys are supported.
 */
public enum RetentionPolicyKind {
    FILES("files"),
    SIZE("size"),
    ENTRIES("entries"),
    HOURS("hours"),
    DAYS("days"),
    BACKUP("backup");
    private final String key;

    RetentionPolicyKind(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }

    public static RetentionPolicyKind fromKey(String key) {
        return switch (key) {
            case "files" -> FILES;
            case "size" -> SIZE;
            case "txs", "entries" -> ENTRIES;
            case "hours" -> HOURS;
            case "days" -> DAYS;
            case "backup" -> BACKUP;
            default ->
                throw new IllegalArgumentException("Invalid log pruning configuration value key: '" + key + "'"
                        + " valid are "
                        + Arrays.stream(RetentionPolicyKind.values())
                                .map(RetentionPolicyKind::key)
                                .collect(Collectors.joining(", ")));
        };
    }
}
