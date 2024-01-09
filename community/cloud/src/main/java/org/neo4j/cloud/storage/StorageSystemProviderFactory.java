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
package org.neo4j.cloud.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Objects;
import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;

/**
 * Factory for providing {@link StorageSystemProvider} instances. These providers MUST have a constructor of the form
 * <code>public ExampleProvider(
 *      ChunkChannelSupplier chunkSupplier,
 *      Config config,
 *      InternalLogProvider logProvider,
 *      MemoryTracker tracker) {...}
 * </code>
 */
@Service
public abstract class StorageSystemProviderFactory {

    @FunctionalInterface
    public interface ChunkChannelSupplier {
        /**
         * @param prefix the store channel prefix for the chunk
         * @return the {@link ChunkChannel} to write to
         * @throws IOException if unable to create the channel
         */
        ChunkChannel create(String prefix) throws IOException;
    }

    /**
     * A channel for writing storage content in chunks
     */
    public interface ChunkChannel extends AutoCloseable {
        Path path();

        void write(ByteBuffer buffer) throws IOException;

        @Override
        void close() throws IOException;
    }

    private final String scheme;

    protected StorageSystemProviderFactory(String scheme) {
        this.scheme = Objects.requireNonNull(scheme);
    }

    /**
     * @return the canonical {@link String} name for the {@link StorageSystemProvider} to be created by this factory.
     * <br>
     * The {@link String} rather than the {@link Class} is returned, so we can lazily load the actual {@link StorageSystemProvider}
     * (and all it's associated cloud JARs). For this to work, implementations of StorageSystemProviderFactory MUST NOT
     * have any imports to these JARs within them.
     */
    protected abstract String storageSystemProviderClass();

    /**
     * @return the scheme of resources that this factory matches
     */
    public String scheme() {
        return scheme;
    }

    /**
     * @param scheme the scheme to check
     * @return <code>true</code> if this factory matches resources with the provided scheme
     */
    public boolean matches(String scheme) {
        return Objects.equals(this.scheme, scheme);
    }

    /**
     * @param chunkSupplier the supplier of chunk channels
     * @param config the configuration
     * @param logProvider the log provider
     * @param memoryTracker the memory tracker to be used by the now {@link StorageSystemProvider}
     * @param classLoader the class loader to use to find the {@link StorageSystemProvider} class and it's dependencies
     * @return a new {@link StorageSystemProvider}
     */
    public StorageSystemProvider createStorageSystemProvider(
            ChunkChannelSupplier chunkSupplier,
            Config config,
            InternalLogProvider logProvider,
            MemoryTracker memoryTracker,
            ClassLoader classLoader) {
        try {
            return loadProviderClass(classLoader)
                    .getConstructor(
                            ChunkChannelSupplier.class, Config.class, InternalLogProvider.class, MemoryTracker.class)
                    .newInstance(chunkSupplier, config, logProvider, memoryTracker);
        } catch (ClassNotFoundException
                | ClassCastException
                | NoSuchMethodException
                | InstantiationException
                | IllegalAccessException
                | InvocationTargetException ex) {
            throw new UncheckedIOException(new IOException(
                    "Unable to create the StorageSystemProvider via the class " + storageSystemProviderClass(), ex));
        }
    }

    @SuppressWarnings("unchecked")
    private Class<StorageSystemProvider> loadProviderClass(ClassLoader classLoader) throws ClassNotFoundException {
        return (Class<StorageSystemProvider>) Class.forName(storageSystemProviderClass(), true, classLoader);
    }
}
