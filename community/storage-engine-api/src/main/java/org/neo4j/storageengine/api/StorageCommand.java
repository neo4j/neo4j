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
package org.neo4j.storageengine.api;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

/**
 * A command representing one unit of change to a {@link StorageEngine}. Commands are created by
 * {@link StorageEngine#createCommands(Collection, ReadableTransactionState, StorageReader, CommandCreationContext, ResourceLocker, LockTracer, long,
 * TxStateVisitor.Decorator,CursorContext, StoreCursors, MemoryTracker)}
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

    KernelVersion version();

    /**
     * Returns a string representation whose user data fields are optionally masked out.
     *
     * @param mask whether to mask data fields
     * @return string representation
     */
    default String toString( Mask mask )
    {
        return toString();
    }

    interface TokenCommand extends StorageCommand
    {
        /**
         * @return the token id in this command.
         */
        int tokenId();

        /**
         * @return {@code true} if the token is internal, {@code false} if this is a public token.
         */
        boolean isInternal();
    }
}
