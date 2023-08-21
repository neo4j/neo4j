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
package org.neo4j.storageengine.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.test.LatestVersions;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class CommandReaderFactoryTestBase<FACTORY extends CommandReaderFactory> {

    private final FACTORY factory;

    protected CommandReaderFactoryTestBase(FACTORY factory) {
        this.factory = factory;
    }

    protected abstract Iterable<KernelVersion> supported();

    protected abstract Iterable<KernelVersion> unsupported();

    @ParameterizedTest
    @MethodSource("unsupported")
    void shouldThrowOnSupportedVersions(KernelVersion kernelVersion) {
        assertThatThrownBy(() -> factory.get(kernelVersion), "acquiring reader for unsupported kernel version")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll("Serialization is not supported", kernelVersion.name());
    }

    @ParameterizedTest
    @MethodSource("supported")
    void shouldProviderReaderWithCorrespondingKernelVersion(KernelVersion kernelVersion) {
        assertReaderHandlesKernelVersions(kernelVersion);
    }

    @Test
    void shouldProviderReaderForGloriousFuture() {
        assertReaderHandlesKernelVersions(KernelVersion.GLORIOUS_FUTURE);
        assertThat(factory.get(KernelVersion.GLORIOUS_FUTURE))
                .as(
                        "reader for %s should be based upon reader for latest kernel version",
                        KernelVersion.GLORIOUS_FUTURE)
                .isInstanceOf(factory.get(LatestVersions.LATEST_KERNEL_VERSION).getClass());
    }

    private void assertReaderHandlesKernelVersions(KernelVersion kernelVersion) {
        assertThat(factory.get(kernelVersion).kernelVersion())
                .as("reader should handle %s", kernelVersion)
                .isEqualTo(kernelVersion);
    }
}
