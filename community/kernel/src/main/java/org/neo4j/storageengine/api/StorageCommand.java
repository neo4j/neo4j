/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.storageengine.api;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

/**
 * A command representing one unit of change to a {@link StorageEngine}. Commands are created by
 * {@link StorageEngine#createCommands(Collection, ReadableTransactionState, StorageStatement, ResourceLocker, long)}
 * and once created can be serialized onto a {@link WritableChannel} and/or passed back to
 * {@link StorageEngine#apply(CommandsToApply, TransactionApplicationMode)} for application where the
 * changes represented by the command are actually applied onto storage.
 */
public interface StorageCommand
{
    /**
     * Serializes change this command represents into a {@link WritableChannel} for later reading back.
     * First byte of command must be type of command.
     *
     * @param channel {@link WritableChannel} to serialize into.
     * @throws IOException I/O error from channel.
     */
    void serialize( WritableChannel channel ) throws IOException;
}
