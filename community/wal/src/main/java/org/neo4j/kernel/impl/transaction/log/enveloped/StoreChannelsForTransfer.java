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
package org.neo4j.kernel.impl.transaction.log.enveloped;

import java.util.List;
import org.neo4j.io.fs.StoreChannel;

/**
 * Contains a list of {@link StoreChannel} for transfer reflecting the range of bytes for the given index range in the
 * log.
 * @param storeChannels    a list of {@link StoreChannel} each channel is positioned at their start and end positions
 *                         for that file's log range. The first channel start is positioned at {@code fromIndex}, all
 *                         other channels are positioned from the start of their first log entry, omitting the log
 *                         header (which for envelopes should be from the second segment). Note: these include
 *                         START_OFFSET envelopes as well.
 * @param toPosition       reflects the end position of {@code toIndex}. This is always for the last channel in the
 *                         list.
 * @param fromIndex        first index in the range
 * @param toIndex          last index in the range
 * @param segmentBlockSize size of one envelope segment in bytes; {@code 0} when the transfer is empty.
 */
public record StoreChannelsForTransfer(
        List<StoreChannel> storeChannels, long toPosition, long fromIndex, long toIndex, int segmentBlockSize) {

    public static StoreChannelsForTransfer nothingToTransfer(long fromIndex, long toIndex) {
        return new StoreChannelsForTransfer(List.of(), -1, fromIndex, toIndex, 0);
    }
}
