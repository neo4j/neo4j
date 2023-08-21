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
package org.neo4j.internal.recordstorage;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.CommandReaderFactoryTestBase;

final class RecordStorageCommandReaderFactoryTest
        extends CommandReaderFactoryTestBase<RecordStorageCommandReaderFactory> {
    private static final Set<KernelVersion> SUPPORTED;
    private static final Set<KernelVersion> UNSUPPORTED;

    static {
        final var supported = KernelVersion.VERSIONS.stream()
                .filter(v -> v.isAtLeast(KernelVersion.EARLIEST))
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(KernelVersion.class)));
        UNSUPPORTED = Collections.unmodifiableSet(EnumSet.complementOf(supported));
        supported.remove(KernelVersion.GLORIOUS_FUTURE);
        SUPPORTED = Collections.unmodifiableSet(supported);
    }

    private RecordStorageCommandReaderFactoryTest() {
        super(RecordStorageCommandReaderFactory.INSTANCE);
    }

    @Override
    protected Iterable<KernelVersion> supported() {
        return SUPPORTED;
    }

    @Override
    protected Iterable<KernelVersion> unsupported() {
        return UNSUPPORTED;
    }
}
