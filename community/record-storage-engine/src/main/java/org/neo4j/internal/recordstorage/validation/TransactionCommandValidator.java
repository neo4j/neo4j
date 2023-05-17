/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.recordstorage.validation;

import static org.neo4j.kernel.impl.store.RecordPageLocationCalculator.pageIdForRecord;
import static org.neo4j.kernel.impl.store.StoreType.STORE_TYPES;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.CommandVisitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidator;

public class TransactionCommandValidator implements CommandVisitor, TransactionValidator {

    private final NeoStores neoStores;
    private final CursorContext cursorContext;
    private final PageCursor[] validationCursors;
    private final EnumMap<StoreType, MutableLongSet> checkedPages;

    public TransactionCommandValidator(NeoStores neoStores, CursorContext cursorContext) {
        this.neoStores = neoStores;
        this.cursorContext = cursorContext;
        this.validationCursors = new PageCursor[STORE_TYPES.length];
        this.checkedPages = new EnumMap<>(StoreType.class);
    }

    @Override
    public void validate(Collection<StorageCommand> commands) {
        try {
            cursorContext.getVersionContext().resetObsoleteHeadState();
            for (StorageCommand command : commands) {
                ((Command) command).handle(this);
            }
        } catch (TransactionConflictException tce) {
            throw tce;
        } catch (Exception e) {
            throw new RuntimeException("Concurrent modification exception.", e);
        } finally {
            closeCursors();
        }
    }

    @Override
    public boolean visitNodeCommand(Command.NodeCommand command) throws IOException {
        checkStore(command.getAfter().getId(), getCursor(StoreType.NODE), StoreType.NODE);
        return false;
    }

    @Override
    public boolean visitRelationshipCommand(Command.RelationshipCommand command) throws IOException {
        checkStore(command.getAfter().getId(), getCursor(StoreType.RELATIONSHIP), StoreType.RELATIONSHIP);
        return false;
    }

    @Override
    public boolean visitPropertyCommand(Command.PropertyCommand command) throws IOException {
        checkStore(command.getAfter().getId(), getCursor(StoreType.PROPERTY), StoreType.PROPERTY);
        return false;
    }

    @Override
    public boolean visitRelationshipGroupCommand(Command.RelationshipGroupCommand command) throws IOException {
        checkStore(command.getAfter().getId(), getCursor(StoreType.RELATIONSHIP_GROUP), StoreType.RELATIONSHIP_GROUP);
        return false;
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand(Command.RelationshipTypeTokenCommand command) throws IOException {
        return false;
    }

    @Override
    public boolean visitLabelTokenCommand(Command.LabelTokenCommand command) throws IOException {
        return false;
    }

    @Override
    public boolean visitPropertyKeyTokenCommand(Command.PropertyKeyTokenCommand command) throws IOException {
        return false;
    }

    @Override
    public boolean visitSchemaRuleCommand(Command.SchemaRuleCommand command) throws IOException {
        return false;
    }

    @Override
    public boolean visitNodeCountsCommand(Command.NodeCountsCommand command) {
        return false;
    }

    @Override
    public boolean visitRelationshipCountsCommand(Command.RelationshipCountsCommand command) {
        return false;
    }

    @Override
    public boolean visitMetaDataCommand(Command.MetaDataCommand command) {
        return false;
    }

    @Override
    public boolean visitGroupDegreeCommand(Command.GroupDegreeCommand command) {
        return false;
    }

    private PageCursor getCursor(StoreType storeType) {
        var cursor = validationCursors[storeType.ordinal()];
        if (cursor != null) {
            return cursor;
        }
        cursor = neoStores.getRecordStore(storeType).openPageCursorForReadingHeadOnly(0, cursorContext);
        validationCursors[storeType.ordinal()] = cursor;
        return cursor;
    }

    private void closeCursors() {
        for (int i = 0; i < validationCursors.length; i++) {
            var cursor = validationCursors[i];
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    private void checkStore(long recordId, PageCursor pageCursor, StoreType storeType) throws IOException {
        var checkedStorePages = checkedPages.get(storeType);
        if (checkedStorePages == null) {
            checkedStorePages = LongSets.mutable.empty();
            checkedPages.put(storeType, checkedStorePages);
        }
        long pageId =
                pageIdForRecord(recordId, neoStores.getRecordStore(storeType).getRecordsPerPage());
        if (checkedStorePages.contains(pageId)) {
            return;
        }
        if (pageCursor.next(pageId)) {
            var versionContext = cursorContext.getVersionContext();
            if (versionContext.obsoleteHeadObserved()) {
                throw new TransactionConflictException(storeType, versionContext);
            }
        }
        checkedStorePages.add(pageId);
    }
}
