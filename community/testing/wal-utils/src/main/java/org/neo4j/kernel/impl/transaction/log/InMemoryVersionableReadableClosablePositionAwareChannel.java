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

import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.V8;

import java.io.IOException;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;

public class InMemoryVersionableReadableClosablePositionAwareChannel extends InMemoryClosableChannel
        implements ReadableLogChannel {
    public InMemoryVersionableReadableClosablePositionAwareChannel() {
        super(true);
    }

    @Override
    public long getLogVersion() {
        return 0;
    }

    @Override
    public LogFormat getLogFormatVersion() {
        return V8;
    }

    @Override
    public void setCurrentPosition(long byteOffset) {
        getCurrentBuffer().position(byteOffset);
    }

    @Override
    public long position() throws IOException {
        return getCurrentLogPosition().getByteOffset();
    }
}
