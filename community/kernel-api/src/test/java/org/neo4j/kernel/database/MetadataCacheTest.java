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
package org.neo4j.kernel.database;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.KernelVersionRepository;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class MetadataCacheTest {
    @Inject
    RandomSupport random;

    @Test
    void shouldProvideGivenKernelVersion() {
        final var kernelVersion = random.among(KernelVersion.VERSIONS);
        final var kernelVersionProvider = (KernelVersionProvider) new MetadataCache(kernelVersion);

        assertThat(kernelVersionProvider.kernelVersion())
                .as("provided kernel version")
                .isEqualTo(kernelVersion);
    }

    @Test
    void shouldProvideKernelVersionFromLogTailMetadata() {
        final var logTailMetadata = randomLogTailMetadata();
        final var kernelVersionProvider = (KernelVersionProvider) new MetadataCache(logTailMetadata);

        assertThat(kernelVersionProvider.kernelVersion())
                .as("provided kernel version")
                .isEqualTo(logTailMetadata.kernelVersion());
    }

    @Test
    void shouldSetGivenKernelVersion() {
        final var kernelVersions = random.ints(2, 0, KernelVersion.VERSIONS.size())
                .mapToObj(KernelVersion.VERSIONS::get)
                .sorted()
                .toList();
        final var initialKernelVersion = kernelVersions.get(0);
        final var kernelVersion = kernelVersions.get(1);

        final var kernelVersionRepository = (KernelVersionRepository) new MetadataCache(initialKernelVersion);
        final var kernelVersionProvider = (KernelVersionProvider) kernelVersionRepository;

        kernelVersionRepository.setKernelVersion(kernelVersion);
        assertThat(kernelVersionProvider.kernelVersion())
                .as("provided kernel version")
                .isEqualTo(kernelVersion);
    }

    private LogTailMetadata randomLogTailMetadata() {
        return new EmptyLogTailMetadata(Config.defaults()) {
            private final KernelVersion kernelVersion = random.among(KernelVersion.VERSIONS);

            @Override
            public KernelVersion kernelVersion() {
                return kernelVersion;
            }
        };
    }
}
