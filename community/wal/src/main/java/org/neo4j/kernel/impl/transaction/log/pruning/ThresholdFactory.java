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
package org.neo4j.kernel.impl.transaction.log.pruning;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.RetentionPolicyKind;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.ThresholdConfigValue;
import org.neo4j.logging.InternalLogProvider;

public final class ThresholdFactory {
    public static final LogPruneThreshold KEEP_ALL = state -> current -> false;
    public static final LogPruneThreshold PRUNE_ALL = state -> current -> true;

    private ThresholdFactory() {}

    public static LogPruneThreshold fromConfigValue(
            FileSystemAbstraction fileSystem, InternalLogProvider logProvider, Clock clock, String configValue) {
        ThresholdConfigValue value = ThresholdConfigParser.parse(configValue);
        if (value == ThresholdConfigValue.NO_PRUNING) {
            return KEEP_ALL;
        }
        return fromKind(fileSystem, logProvider, clock, value);
    }

    public static LogPruneThreshold fromKind(
            FileSystemAbstraction fileSystem,
            InternalLogProvider logProvider,
            Clock clock,
            ThresholdConfigValue configuredThreshold) {
        long value = configuredThreshold.value();
        return switch (RetentionPolicyKind.fromKey(configuredThreshold.type())) {
            case FILES -> new FileCountThreshold(value, logProvider);
            case SIZE -> new FileSizeThreshold(fileSystem, value, logProvider);
            case ENTRIES -> new EntryCountThreshold(logProvider, value);
            case HOURS -> timeBased(fileSystem, logProvider, clock, configuredThreshold, HOURS);
            case DAYS -> timeBased(fileSystem, logProvider, clock, configuredThreshold, DAYS);
            case BACKUP ->
                new BackupThreshold(
                        logProvider,
                        configuredThreshold.hasAdditionalRestriction()
                                ? configuredThreshold.additionalRestriction()
                                : 0,
                        new FileSizeThreshold(fileSystem, value, logProvider));
        };
    }

    private static EntryTimespanThreshold timeBased(
            FileSystemAbstraction fileSystem,
            InternalLogProvider logProvider,
            Clock clock,
            ThresholdConfigValue configured,
            TimeUnit timeUnit) {
        if (configured.hasAdditionalRestriction()) {
            return new EntryTimespanThreshold(
                    logProvider,
                    clock,
                    timeUnit,
                    configured.value(),
                    new FileSizeThreshold(fileSystem, configured.additionalRestriction(), logProvider));
        }
        return new EntryTimespanThreshold(logProvider, clock, timeUnit, configured.value());
    }
}
