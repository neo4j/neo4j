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
package org.neo4j.kernel.api.impl.fulltext;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.test.OtherThreadExecutor.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.values.storable.Values;

class IndexUpdateSinkTest {
    private IndexDescriptor descriptor;
    private DatabaseIndex<?> index;
    private OnDemandJobScheduler scheduler;
    private OtherThreadExecutor t2;

    @BeforeEach
    void setUp() {
        descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2))
                .withName("my_index")
                .materialise(1);
        index = Mockito.mock(DatabaseIndex.class);
        when(index.getDescriptor()).thenReturn(descriptor);
        scheduler = new OnDemandJobScheduler();
        t2 = new OtherThreadExecutor("T2");
    }

    @AfterEach
    void stop() {
        t2.close();
        scheduler.close();
    }

    @Test
    void enqueueShouldAwaitQueueSpace() throws Exception {
        // given
        var sink = new IndexUpdateSink(scheduler, 100);
        var updater1 = updater();
        var updater2 = updater();
        var updater3 = updater();
        sink.enqueueTransactionBatchOfUpdates(index, updater1, updates(40));
        sink.enqueueTransactionBatchOfUpdates(index, updater2, updates(40));

        // when
        var thirdEnqueueFuture =
                t2.executeDontWait(command(() -> sink.enqueueTransactionBatchOfUpdates(index, updater3, updates(40))));
        t2.waitUntilWaiting(location -> location.isAt(IndexUpdateSink.class, "enqueueTransactionBatchOfUpdates"));

        // then
        scheduler.runJob();
        verify(updater1).close();
        verify(updater2).close();
        thirdEnqueueFuture.get();
        scheduler.runJob();
        verify(updater3).close();
    }

    @Test
    void shouldAwaitUpdatesToBeApplied() throws Exception {
        // given
        var sink = new IndexUpdateSink(scheduler, 100);
        var updater = updater();
        sink.enqueueTransactionBatchOfUpdates(index, updater, updates(10));

        // when
        var awaitUpdateFuture = t2.executeDontWait(command(sink::awaitUpdateApplication));
        t2.waitUntilWaiting(location -> location.isAt(IndexUpdateSink.class, "awaitUpdateApplication"));
        verifyNoInteractions(updater);
        scheduler.runJob();
        awaitUpdateFuture.get();

        // then
        verify(updater).close();
    }

    private Collection<IndexEntryUpdate<?>> updates(int count) {
        List<IndexEntryUpdate<?>> updates = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            updates.add(IndexEntryUpdate.add(i, descriptor, Values.intValue(i)));
        }
        return updates;
    }

    private IndexUpdater updater() {
        return Mockito.mock(IndexUpdater.class);
    }
}
