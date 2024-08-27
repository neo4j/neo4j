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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionRepository;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.test.LatestVersions;

public class KernelVersionTransactionApplierTest {

    @Test
    void shouldUpdateKernelVersion() throws Exception {
        final var from = KernelVersion.EARLIEST;
        final var to = LatestVersions.LATEST_KERNEL_VERSION;
        assumeThat(to).isGreaterThan(from);

        final var kernelVersionRepository = mock(KernelVersionRepository.class);
        when(kernelVersionRepository.kernelVersion()).thenReturn(from);

        // given  a command to update the kernel version
        final var applier = new KernelVersionTransactionApplierFactory(kernelVersionRepository);
        final var command = createMetaDataCommand(from, to);
        final var txToApply = mock(StorageEngineTransaction.class);

        // when   command applied
        final var result = CommandHandlerContract.apply(applier, command::handle, txToApply);

        // then   it is successful (false) and the version updated
        assertThat(result).isFalse();
        verify(kernelVersionRepository).setKernelVersion(LatestVersions.LATEST_KERNEL_VERSION);
    }

    private static Command.MetaDataCommand createMetaDataCommand(KernelVersion from, KernelVersion to) {
        MetaDataRecord before = new MetaDataRecord();
        before.initialize(true, from.version());

        MetaDataRecord after = new MetaDataRecord();
        after.initialize(true, to.version());

        return new Command.MetaDataCommand(RecordStorageCommandReaderFactory.INSTANCE.get(to), before, after);
    }
}
