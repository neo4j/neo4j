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
package org.neo4j.internal.recordstorage;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.CommandReaderFactory;

public class RecordStorageCommandReaderFactory implements CommandReaderFactory {
    public static final RecordStorageCommandReaderFactory INSTANCE = new RecordStorageCommandReaderFactory();

    @Override
    public LogCommandSerialization get(KernelVersion version) {
        return switch (version) {
            case V4_0 ->
                throw new IllegalStateException("Serialization is not supported for legacy format version " + version);
            case V4_2 -> LogCommandSerializationV4_2.INSTANCE;
            case V4_3_D4 -> LogCommandSerializationV4_3_D3.INSTANCE;
            case V4_4 -> LogCommandSerializationV4_3_D3.V4_4_INSTANCE;
            case V5_0 -> LogCommandSerializationV5_0.INSTANCE;
            case V5_7 -> LogCommandSerializationV5_0.V5_7_INSTANCE;
            case V5_8 -> LogCommandSerializationV5_8.INSTANCE;
            case V5_9 -> LogCommandSerializationV5_8.V5_9_INSTANCE;
            case V5_10 -> LogCommandSerializationV5_8.V5_10_INSTANCE;
            case V5_11 -> LogCommandSerializationV5_11.INSTANCE;
            case V5_12 -> LogCommandSerializationV5_11.V5_12_INSTANCE;
            case V5_13 -> LogCommandSerializationV5_11.V5_13_INSTANCE;
            case V5_14 -> LogCommandSerializationV5_11.V5_14_INSTANCE;
            case V5_15 -> LogCommandSerializationV5_11.V5_15_INSTANCE;
            case V5_18 -> LogCommandSerializationV5_11.V5_18_INSTANCE;
            case V5_19 -> LogCommandSerializationV5_11.V5_19_INSTANCE;
            case V5_20 -> LogCommandSerializationV5_11.V5_20_INSTANCE;
            case V5_22 -> LogCommandSerializationV5_11.V5_22_INSTANCE;
            case V5_23 -> LogCommandSerializationV5_11.V5_23_INSTANCE;
            case V5_25 -> LogCommandSerializationV5_25.INSTANCE;
            case V2025_04 -> LogCommandSerializationV5_25.V2025_04_INSTANCE;
            case V2025_05 -> LogCommandSerializationV5_25.V2025_05_INSTANCE;
            case V2025_07 -> LogCommandSerializationV5_25.V2025_07_INSTANCE;
            case V2025_08 -> LogCommandSerializationV5_25.V2025_08_INSTANCE;
            case V2025_09 -> LogCommandSerializationV5_25.V2025_09_INSTANCE;
            case V2025_10 -> LogCommandSerializationV5_25.V2025_10_INSTANCE;
            case V2025_11 -> LogCommandSerializationV5_25.V2025_11_INSTANCE;
            case V2026_01 -> LogCommandSerializationV5_25.V2026_01_INSTANCE;
            case V2026_02 -> LogCommandSerializationV5_25.V2026_02_INSTANCE;
            case GLORIOUS_FUTURE -> LogCommandSerializationVGloriousFuture.INSTANCE;
        };
    }
}
