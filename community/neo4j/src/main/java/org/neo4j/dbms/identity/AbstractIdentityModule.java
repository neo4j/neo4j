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
package org.neo4j.dbms.identity;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.diagnostics.DiagnosticsProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.state.SimpleFileStorage;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.util.Id;

public abstract class AbstractIdentityModule extends LifecycleAdapter implements DiagnosticsProvider, ServerIdentity {
    protected static <T extends Id> T readOrGenerate(
            SimpleStorage<T> storage,
            InternalLog log,
            Class<T> type,
            Function<UUID, ? extends T> creator,
            Supplier<UUID> uuid) {
        T myself;
        try {
            if (storage.exists()) {
                myself = storage.readState();
                if (myself == null) {
                    throw new IllegalStateException(String.format(
                            "%s storage was found on disk, but it could not be read correctly", type.getSimpleName()));
                } else {
                    log.info(String.format("Found %s on disk: %s (%s)", type.getSimpleName(), myself, myself.uuid()));
                }
            } else {
                UUID newUuid = uuid.get();
                myself = creator.apply(newUuid);
                storage.writeState(myself);

                log.info(String.format("Generated new %s: %s (%s)", type.getSimpleName(), myself, newUuid));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return myself;
    }

    public static SimpleStorage<ServerId> createServerIdStorage(FileSystemAbstraction fs, Path serverIdFile) {
        return new SimpleFileStorage<>(fs, serverIdFile, ServerIdMarshal.INSTANCE);
    }

    @Override
    public String getDiagnosticsName() {
        return "Global Server Identity";
    }

    @Override
    public void dump(DiagnosticsLogger logger) {
        logger.log(String.format("Registered %s", serverId()));
    }
}
