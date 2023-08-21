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

import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.entry.v57.ChunkEndLogEntrySerializerV5_7;
import org.neo4j.kernel.impl.transaction.log.entry.v57.ChunkStartLogEntrySerializerV5_7;
import org.neo4j.kernel.impl.transaction.log.entry.v57.DetachedCheckpointLogEntrySerializerV5_7;
import org.neo4j.kernel.impl.transaction.log.entry.v57.RollbackLogEntrySerializerV5_7;

class LogEntrySerializationSetV5_7 extends LogEntrySerializationSetV5_0 {
    LogEntrySerializationSetV5_7() {
        this(KernelVersion.V5_7);
    }

    LogEntrySerializationSetV5_7(KernelVersion kernelVersion) {
        super(kernelVersion);
        register(new ChunkStartLogEntrySerializerV5_7());
        register(new ChunkEndLogEntrySerializerV5_7());

        register(new DetachedCheckpointLogEntrySerializerV5_7(), true);

        register(new RollbackLogEntrySerializerV5_7());
    }
}
