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
package org.neo4j.internal.recordstorage.validation;

import static java.lang.System.lineSeparator;
import static java.util.Collections.emptyMap;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.multi_version_dump_transaction_validation_page_locks;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.multi_version_transaction_validation_fail_fast;
import static org.neo4j.kernel.impl.store.RecordPageLocationCalculator.pageIdForRecord;
import static org.neo4j.kernel.impl.store.StoreType.STORE_TYPES;
import static org.neo4j.lock.ResourceType.PAGE;
import static org.neo4j.storageengine.api.txstate.validation.TransactionValidationResource.EMPTY_VALIDATION_RESOURCE;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.CommandVisitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.monitoring.TransactionMonitor;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.txstate.validation.TransactionConflictException;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidationResource;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidator;

public class TransactionCommandValidator implements CommandVisitor, TransactionValidator {

    private static final int PAGE_ID_BITS = 54;
    private static final long PAGE_ID_MASK = (1L << PAGE_ID_BITS) - 1;
    private final NeoStores neoStores;
    private final LockManager.Client validationLockClient;
    private final MemoryTracker memoryTracker;
    private final Config config;
    private final TransactionMonitor transactionMonitor;
    private final PageCursor[] validationCursors;
    private final Log log;
    private final EnumMap<StoreType, MutableLongSet> checkedPages;
    private LockTracer lockTracer;
    private CursorContext cursorContext;
    private boolean dumpLocks;
    private boolean failFast;
    private Map<PageEntry, Long> observedPageVersions;

    public TransactionCommandValidator(
            NeoStores neoStores,
            LockManager lockManager,
            MemoryTracker memoryTracker,
            Config config,
            LogProvider logProvider,
            TransactionMonitor transactionMonitor) {
        this.neoStores = neoStores;
        this.validationLockClient = lockManager.newClient();
        this.memoryTracker = memoryTracker;
        this.config = config;
        this.validationCursors = new PageCursor[STORE_TYPES.length];
        this.checkedPages = new EnumMap<>(StoreType.class);
        this.transactionMonitor = transactionMonitor;
        this.log = logProvider.getLog(getClass());
    }

    @Override
    public TransactionValidationResource validate(
            Collection<StorageCommand> commands,
            long transactionSequenceNumber,
            CursorContext cursorContext,
            LeaseClient leaseClient,
            LockTracer lockTracer) {
        try {
            if (commands.isEmpty()) {
                return EMPTY_VALIDATION_RESOURCE;
            }
            initValidation(transactionSequenceNumber, cursorContext, leaseClient, lockTracer);

            cursorContext.getVersionContext().resetObsoleteHeadState();
            for (StorageCommand command : commands) {
                ((Command) command).handle(this);
            }

            TransactionValidationResource closeLocksResult = validationLockClient::close;
            return dumpLocks
                    ? new DumpStateResourceWrapper(
                            closeLocksResult, validationLockClient, log, neoStores, observedPageVersions)
                    : closeLocksResult;
        } catch (TransactionConflictException tce) {
            validationLockClient.close();
            throw tce;
        } catch (Exception e) {
            validationLockClient.close();
            throw new TransactionConflictException(e);
        } finally {
            closeCursors();
        }
    }

    private void initValidation(
            long txSequenceNumber, CursorContext cursorContext, LeaseClient leaseClient, LockTracer lockTracer) {
        this.cursorContext = cursorContext;
        this.lockTracer = lockTracer;
        this.dumpLocks = config.get(multi_version_dump_transaction_validation_page_locks);
        this.failFast = config.get(multi_version_transaction_validation_fail_fast);
        this.observedPageVersions = dumpLocks ? new HashMap<>() : emptyMap();
        validationLockClient.initialize(leaseClient, txSequenceNumber, memoryTracker, config);
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
        PropertyRecord propertyRecord = command.getAfter();
        checkStore(propertyRecord.getId(), getCursor(StoreType.PROPERTY), StoreType.PROPERTY);
        if (propertyRecord.inUse()) {
            for (PropertyBlock block : propertyRecord) {
                if (!block.isLight() && block.getValueRecords().get(0).isCreated()) {
                    checkDynamicRecords(block.getValueRecords());
                }
            }
        }
        checkDynamicRecords(propertyRecord.getDeletedRecords());

        return false;
    }

    private void checkDynamicRecords(List<DynamicRecord> records) throws IOException {
        PageCursor stringCursor = null;
        PageCursor arrayCursor = null;

        for (DynamicRecord valueRecord : records) {
            PropertyType recordType = valueRecord.getType();
            if (recordType == PropertyType.STRING) {
                if (stringCursor == null) {
                    stringCursor = getCursor(StoreType.PROPERTY_STRING);
                }
                checkStore(valueRecord.getId(), stringCursor, StoreType.PROPERTY_STRING);
            } else if (recordType == PropertyType.ARRAY) {
                if (arrayCursor == null) {
                    arrayCursor = getCursor(StoreType.PROPERTY_ARRAY);
                }
                checkStore(valueRecord.getId(), arrayCursor, StoreType.PROPERTY_ARRAY);
            } else {
                throw new InvalidRecordException("Not supported record type for validation: " + valueRecord);
            }
        }
    }

    @Override
    public boolean visitRelationshipGroupCommand(Command.RelationshipGroupCommand command) throws IOException {
        checkStore(command.getAfter().getId(), getCursor(StoreType.RELATIONSHIP_GROUP), StoreType.RELATIONSHIP_GROUP);
        return false;
    }

    @Override
    public boolean visitSchemaRuleCommand(Command.SchemaRuleCommand command) throws IOException {
        checkStore(command.getAfter().getId(), getCursor(StoreType.SCHEMA), StoreType.SCHEMA);
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
        checkedPages.clear();
        for (int i = 0; i < validationCursors.length; i++) {
            var cursor = validationCursors[i];
            if (cursor != null) {
                cursor.close();
            }
        }
        Arrays.fill(validationCursors, null);
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

        var versionContext = cursorContext.getVersionContext();
        long resourceId = pageId | ((long) storeType.ordinal() << PAGE_ID_BITS);
        if (failFast && !validationLockClient.tryExclusiveLock(PAGE, resourceId)) {
            throw new TransactionConflictException(storeType.getDatabaseFile(), pageId);
        } else {
            validationLockClient.acquireExclusive(lockTracer, PAGE, resourceId);
        }
        if (pageCursor.next(pageId)) {
            if (versionContext.invisibleHeadObserved()) {
                transactionMonitor.transactionValidationFailure(storeType.getDatabaseFile());
                throw new TransactionConflictException(storeType.getDatabaseFile(), versionContext, pageId);
            }
        }
        checkedStorePages.add(pageId);

        if (dumpLocks) {
            storyPageInfo(storeType, pageId, versionContext);
        }
    }

    private void storyPageInfo(StoreType storeType, long pageId, VersionContext versionContext) {
        long chainHead = versionContext.chainHeadVersion();
        observedPageVersions.put(new PageEntry(pageId, storeType), chainHead);
    }

    private static class DumpStateResourceWrapper implements TransactionValidationResource {
        private static final long UNKNOWN_PAGE_VERSION = -1;
        private final Resource delegate;
        private final LockManager.Client lockClient;
        private final Log log;
        private final NeoStores neoStores;
        private int chunkNumber;
        private long txId;
        private final Map<PageEntry, Long> observedVersions;

        private DumpStateResourceWrapper(
                Resource delegate,
                LockManager.Client lockClient,
                Log log,
                NeoStores neoStores,
                Map<PageEntry, Long> observedVersions) {
            this.delegate = delegate;
            this.lockClient = lockClient;
            this.log = log;
            this.neoStores = neoStores;
            this.observedVersions = observedVersions;
        }

        @Override
        public void close() {
            dumpLockedPagesInfo();
            delegate.close();
        }

        private void dumpLockedPagesInfo() {
            StringBuilder locksDumpBuilder = new StringBuilder();
            locksDumpBuilder
                    .append("Transaction sequence number: ")
                    .append(lockClient.getTransactionId())
                    .append(" with tx id(chunk): ")
                    .append(txId)
                    .append("(")
                    .append(chunkNumber)
                    .append(")");
            var locks = lockClient.activeLocks();
            if (locks.isEmpty()) {
                locksDumpBuilder.append(" does not have any validation page locks.");
            } else {
                locksDumpBuilder.append(" locked page(s):").append(lineSeparator());

                var storyTypeRecords = new EnumMap<StoreType, Integer>(StoreType.class);

                for (ActiveLock activeLock : locks) {
                    long resourceId = activeLock.resourceId();
                    var storeType = StoreType.values()[(int) (resourceId >> PAGE_ID_BITS)];
                    int recordsPerPage = storyTypeRecords.computeIfAbsent(
                            storeType, type -> neoStores.getRecordStore(type).getRecordsPerPage());
                    long pageId = resourceId & PAGE_ID_MASK;
                    locksDumpBuilder
                            .append(pageId)
                            .append(" of ")
                            .append(storeType)
                            .append(" store, with records per page ")
                            .append(recordsPerPage)
                            .append(" observed page version: ")
                            .append(observedVersions.getOrDefault(
                                    new PageEntry(pageId, storeType), UNKNOWN_PAGE_VERSION))
                            .append(lineSeparator());
                }
            }
            log.error(locksDumpBuilder.toString());
        }

        @Override
        public void chunkAppended(int chunkNumber, long txId) {
            this.chunkNumber = chunkNumber;
            this.txId = txId;
        }
    }

    private record PageEntry(long id, StoreType type) {}
}
