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
package org.neo4j.kernel.impl.transaction.log.entry;

import static java.lang.String.format;

import java.util.EnumMap;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;

public class LogEntrySerializationSets {
    private static final EnumMap<KernelVersion, LogEntrySerializationSet> SERIALIZATION_SETS =
            new EnumMap<>(KernelVersion.class);

    static {
        SERIALIZATION_SETS.put(KernelVersion.V2_3, new LogEntrySerializationSetV2_3());
        SERIALIZATION_SETS.put(KernelVersion.V4_0, new LogEntrySerializationSetV4_0());
        SERIALIZATION_SETS.put(KernelVersion.V4_2, new LogEntrySerializationSetV4_2());
        SERIALIZATION_SETS.put(KernelVersion.V4_3_D4, new LogEntrySerializationSetV4_3());
        SERIALIZATION_SETS.put(KernelVersion.V4_4, new LogEntrySerializationSetV4_4());
        SERIALIZATION_SETS.put(KernelVersion.V5_0, new LogEntrySerializationSetV5_0());
        SERIALIZATION_SETS.put(KernelVersion.V5_7, new LogEntrySerializationSetV5_7());
        SERIALIZATION_SETS.put(KernelVersion.V5_8, new LogEntrySerializationSetV5_8());
        SERIALIZATION_SETS.put(KernelVersion.V5_9, new LogEntrySerializationSetV5_9());
        SERIALIZATION_SETS.put(KernelVersion.V5_10, new LogEntrySerializationSetV5_10());
        SERIALIZATION_SETS.put(KernelVersion.V5_11, new LogEntrySerializationSetV5_11());
        SERIALIZATION_SETS.put(KernelVersion.V5_12, new LogEntrySerializationSetV5_12());
        SERIALIZATION_SETS.put(KernelVersion.V5_13, new LogEntrySerializationSetV5_13());
        SERIALIZATION_SETS.put(KernelVersion.V5_14, new LogEntrySerializationSetV5_14());
        SERIALIZATION_SETS.put(KernelVersion.V5_15, new LogEntrySerializationSetV5_15());
        SERIALIZATION_SETS.put(KernelVersion.V5_18, new LogEntrySerializationSetV5_18());
        SERIALIZATION_SETS.put(KernelVersion.V5_19, new LogEntrySerializationSetV5_19());
        SERIALIZATION_SETS.put(KernelVersion.V5_20, new LogEntrySerializationSetV5_20());
    }

    /**
     * @param version the {@link KernelVersion} to get the {@link LogEntrySerializationSet} for. The returned serializer is capable of reading
     * and writing all types of log entries.
     * @return LogEntrySerializationSet for the given {@code version}.
     */
    public static LogEntrySerializationSet serializationSet(
            KernelVersion version, BinarySupportedKernelVersions binarySupportedKernelVersions) {
        LogEntrySerializationSet parserSet = SERIALIZATION_SETS.get(version);
        if (parserSet == null) {
            if (version == KernelVersion.GLORIOUS_FUTURE
                    && binarySupportedKernelVersions.latestSupportedIsAtLeast(version)) {
                return new LogEntrySerializationSetVGloriousFuture();
            }
            throw new IllegalArgumentException(format("No log entries version matching %s", version));
        }
        return parserSet;
    }
}
