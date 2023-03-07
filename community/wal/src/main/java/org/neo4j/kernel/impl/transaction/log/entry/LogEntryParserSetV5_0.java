/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.entry.v50.DetachedCheckpointLogEntryParserV5_0;

public class LogEntryParserSetV5_0 extends LogEntryParserSetV4_4 {

    LogEntryParserSetV5_0() {
        this(KernelVersion.V5_0);
    }

    LogEntryParserSetV5_0(KernelVersion kernelVersion) {
        super(kernelVersion);
        register(new DetachedCheckpointLogEntryParserV5_0());
    }

    @Override
    public ReadableChannel wrap(ReadableChannel channel) {
        return channel;
    }
}
