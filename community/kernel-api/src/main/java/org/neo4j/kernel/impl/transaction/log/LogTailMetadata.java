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

import java.util.Optional;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.storageengine.api.StoreId;

public interface LogTailMetadata extends KernelVersionProvider, LogTailLogVersionsMetadata {

    Optional<StoreId> getStoreId();

    boolean logsMissing();

    boolean hasUnreadableBytesInCheckpointLogs();

    Optional<CheckpointInfo> getLastCheckPoint();
}
