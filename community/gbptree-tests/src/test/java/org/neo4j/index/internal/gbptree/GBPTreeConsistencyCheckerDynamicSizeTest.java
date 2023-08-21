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
package org.neo4j.index.internal.gbptree;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.index.internal.gbptree.GBPTreeCorruption.notAnOffloadNode;
import static org.neo4j.index.internal.gbptree.GBPTreeCorruption.pageSpecificCorruption;
import static org.neo4j.index.internal.gbptree.GBPTreeTestUtil.consistencyCheck;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.junit.jupiter.api.Test;

public class GBPTreeConsistencyCheckerDynamicSizeTest extends GBPTreeConsistencyCheckerTestBase<RawBytes, RawBytes> {
    @Override
    protected TestLayout<RawBytes, RawBytes> getLayout() {
        return new SimpleByteArrayLayout(true);
    }

    @Test
    void offloadPointerPointToNonOffloadPage() throws IOException {
        try (GBPTree<RawBytes, RawBytes> index = index().build()) {
            int keySize = index.inlineKeyValueSizeCap();
            RawBytes key = key(keySize + 1);
            RawBytes value = value(0);
            try (Writer<RawBytes, RawBytes> writer = index.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                writer.put(key, value);
            }

            GBPTreeInspection inspection = inspect(index);
            ImmutableLongList offloadNodes = inspection.single().offloadNodes();
            long offloadNode = offloadNodes.get(random.nextInt(offloadNodes.size()));

            index.unsafe(pageSpecificCorruption(offloadNode, notAnOffloadNode()), NULL_CONTEXT);

            assertReportException(index);
        }
    }

    private RawBytes key(int keySize, byte... firstBytes) {
        RawBytes key = layout.newKey();
        key.bytes = new byte[keySize];
        for (int i = 0; i < firstBytes.length && i < keySize; i++) {
            key.bytes[i] = firstBytes[i];
        }
        return key;
    }

    private RawBytes value(int valueSize) {
        RawBytes value = layout.newValue();
        value.bytes = new byte[valueSize];
        return value;
    }

    private static <KEY, VALUE> void assertReportException(GBPTree<KEY, VALUE> index) {
        MutableBoolean called = new MutableBoolean();
        consistencyCheck(index, new GBPTreeConsistencyCheckVisitor.Adaptor() {
            @Override
            public void exception(Exception e) {
                called.setTrue();
                assertThat(e.getMessage())
                        .contains("Tried to read from offload store but page is not an offload page.");
            }
        });
        assertCalled(called);
    }
}
