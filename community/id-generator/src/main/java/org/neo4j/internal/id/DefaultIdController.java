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
package org.neo4j.internal.id;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.memory.MemoryTracker;

/**
 * Default implementation of {@link IdController}.
 * Do not add any additional possibilities or functionality.
 */
public class DefaultIdController extends LifecycleAdapter implements IdController {
    public DefaultIdController() {}

    @Override
    public void maintenance() {}

    @Override
    public void initialize(
            FileSystemAbstraction fs,
            Path baseBufferPath,
            Config config,
            Supplier<TransactionSnapshot> snapshotSupplier,
            IdFreeCondition condition,
            MemoryTracker memoryTracker,
            DatabaseReadOnlyChecker databaseReadOnlyChecker)
            throws IOException {}
}
