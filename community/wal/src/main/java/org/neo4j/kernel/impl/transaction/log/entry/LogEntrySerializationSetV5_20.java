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
import org.neo4j.kernel.impl.transaction.log.entry.v520.ChunkStartLogEntrySerializerV5_20;
import org.neo4j.kernel.impl.transaction.log.entry.v520.DetachedCheckpointLogEntrySerializerV5_20;
import org.neo4j.kernel.impl.transaction.log.entry.v520.RollbackLogEntrySerializerV5_20;
import org.neo4j.kernel.impl.transaction.log.entry.v520.StartLogEntrySerializerV5_20;

class LogEntrySerializationSetV5_20 extends LogEntrySerializationSetV5_19 {
    LogEntrySerializationSetV5_20() {
        this(KernelVersion.V5_20);
    }

    LogEntrySerializationSetV5_20(KernelVersion kernelVersion) {
        super(kernelVersion);

        register(new DetachedCheckpointLogEntrySerializerV5_20(), true);

        register(new ChunkStartLogEntrySerializerV5_20(), true);
        register(new RollbackLogEntrySerializerV5_20(), true);
        register(new StartLogEntrySerializerV5_20(), true);
    }
}
