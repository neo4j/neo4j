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
package org.neo4j.kernel.api.database.transaction;

import java.io.IOException;
import java.util.List;
import org.neo4j.io.IOUtils;

/**
 * List of transaction log channels and associated last closed transaction ids.
 * Channels come in a list. Channel that is before another channel in a list, is also before that channel in actual transactions logs.
 * Provided channels are read only and can be closed at any time by the provided service.
 */
public class TransactionLogChannels implements AutoCloseable {
    private final List<LogChannel> channels;

    public TransactionLogChannels(List<LogChannel> channels) {
        this.channels = channels;
    }

    public List<LogChannel> getChannels() {
        return channels;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeAll(channels);
    }
}
