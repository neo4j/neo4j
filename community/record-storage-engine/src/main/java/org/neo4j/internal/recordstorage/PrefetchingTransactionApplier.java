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

import static org.neo4j.kernel.impl.store.RecordPageLocationCalculator.pageIdForRecord;
import static org.neo4j.kernel.impl.store.StoreType.STORE_TYPES;

import java.io.IOException;
import java.util.Collection;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.internal.recordstorage.Command.BaseCommand;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.PropertyCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipGroupCommand;
import org.neo4j.io.pagecache.prefetch.PagePrefetcher;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;

public class PrefetchingTransactionApplier extends TransactionApplier.Adapter {
    private final NeoStores neoStores;
    private final PagePrefetcher prefetcher;
    private final MutableLongSet[] checkedPages;

    public PrefetchingTransactionApplier(NeoStores neoStores, PagePrefetcher prefetcher) {
        this.neoStores = neoStores;
        this.prefetcher = prefetcher;
        this.checkedPages = new MutableLongSet[STORE_TYPES.length];
    }

    @Override
    public boolean visitNodeCommand(NodeCommand command) {
        track(command, StoreType.NODE);
        track(command.getAfter().getDynamicLabelRecords(), StoreType.NODE_LABEL);
        return false;
    }

    @Override
    public boolean visitRelationshipCommand(RelationshipCommand command) {
        track(command, StoreType.RELATIONSHIP);
        return false;
    }

    @Override
    public boolean visitPropertyCommand(PropertyCommand command) {
        track(command, StoreType.PROPERTY);
        for (PropertyBlock block : command.getAfter().propertyBlocks()) {
            switch (block.getType()) {
                case STRING -> track(block.getValueRecords(), StoreType.PROPERTY_STRING);
                case ARRAY -> track(block.getValueRecords(), StoreType.PROPERTY_ARRAY);
                default -> {}
            }
        }
        return false;
    }

    @Override
    public boolean visitRelationshipGroupCommand(RelationshipGroupCommand command) {
        track(command, StoreType.RELATIONSHIP_GROUP);
        return false;
    }

    private void track(AbstractBaseRecord record, StoreType storeType) {
        pagesForStore(storeType)
                .add(pageIdForRecord(
                        record.getId(), neoStores.getRecordStore(storeType).getRecordsPerPage()));
    }

    private MutableLongSet pagesForStore(StoreType storeType) {
        int position = storeType.ordinal();
        var checkedStorePages = checkedPages[position];
        if (checkedStorePages == null) {
            checkedStorePages = LongSets.mutable.empty();
            checkedPages[position] = checkedStorePages;
        }
        return checkedStorePages;
    }

    private <RECORD extends AbstractBaseRecord> void track(BaseCommand<RECORD> command, StoreType storeType) {
        track(command.getAfter(), storeType);
    }

    private void track(Collection<? extends AbstractBaseRecord> records, StoreType storeType) {
        for (AbstractBaseRecord record : records) {
            track(record, storeType);
        }
    }

    @Override
    public void close() throws IOException {
        for (StoreType storeType : StoreType.STORE_TYPES) {
            int index = storeType.ordinal();
            var pages = checkedPages[index];
            if (pages != null) {
                prefetcher.submit(neoStores.getRecordStore(storeType).getStoreFile(), pages.toSortedArray());
            }
        }
    }
}
