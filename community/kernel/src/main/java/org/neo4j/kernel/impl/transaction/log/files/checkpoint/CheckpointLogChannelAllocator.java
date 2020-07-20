/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogChannelAllocator;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;

import static org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor.EMPTY_ACCESSOR;

/**
 * While technically checkpoint log files do not need store id to be present in file header its easier to reuse the same
 * log header as other files
 */
public class CheckpointLogChannelAllocator extends TransactionLogChannelAllocator
{
    public CheckpointLogChannelAllocator( TransactionLogFilesContext logFilesContext, TransactionLogFilesHelper fileHelper )
    {
        super( logFilesContext, fileHelper, new LogHeaderCache( 1 ), EMPTY_ACCESSOR );
    }
}
