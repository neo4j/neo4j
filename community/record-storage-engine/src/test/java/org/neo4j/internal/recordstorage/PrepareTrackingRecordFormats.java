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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.storageengine.api.StoreFormatLimits;
import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

public class PrepareTrackingRecordFormats implements RecordFormats {
    private final RecordFormats actual;
    private final Set<NodeRecord> nodePrepare = new HashSet<>();
    private final Set<RelationshipRecord> relationshipPrepare = new HashSet<>();
    private final Set<RelationshipGroupRecord> relationshipGroupPrepare = new HashSet<>();
    private final Set<PropertyRecord> propertyPrepare = new HashSet<>();
    private final Set<SchemaRecord> schemaPrepare = new HashSet<>();
    private final Set<DynamicRecord> dynamicPrepare = new HashSet<>();
    private final Set<PropertyKeyTokenRecord> propertyKeyTokenPrepare = new HashSet<>();
    private final Set<LabelTokenRecord> labelTokenPrepare = new HashSet<>();
    private final Set<RelationshipTypeTokenRecord> relationshipTypeTokenPrepare = new HashSet<>();
    private final Set<MetaDataRecord> metaDataPrepare = new HashSet<>();

    public PrepareTrackingRecordFormats(RecordFormats actual) {
        this.actual = actual;
    }

    @Override
    public String introductionVersion() {
        return actual.introductionVersion();
    }

    @Override
    public PrepareTrackingRecordFormat<NodeRecord> node() {
        return new PrepareTrackingRecordFormat<>(actual.node(), nodePrepare);
    }

    @Override
    public PrepareTrackingRecordFormat<RelationshipGroupRecord> relationshipGroup() {
        return new PrepareTrackingRecordFormat<>(actual.relationshipGroup(), relationshipGroupPrepare);
    }

    @Override
    public PrepareTrackingRecordFormat<RelationshipRecord> relationship() {
        return new PrepareTrackingRecordFormat<>(actual.relationship(), relationshipPrepare);
    }

    @Override
    public PrepareTrackingRecordFormat<PropertyRecord> property() {
        return new PrepareTrackingRecordFormat<>(actual.property(), propertyPrepare);
    }

    @Override
    public RecordFormat<SchemaRecord> schema() {
        return new PrepareTrackingRecordFormat<>(actual.schema(), schemaPrepare);
    }

    @Override
    public PrepareTrackingRecordFormat<LabelTokenRecord> labelToken() {
        return new PrepareTrackingRecordFormat<>(actual.labelToken(), labelTokenPrepare);
    }

    @Override
    public PrepareTrackingRecordFormat<PropertyKeyTokenRecord> propertyKeyToken() {
        return new PrepareTrackingRecordFormat<>(actual.propertyKeyToken(), propertyKeyTokenPrepare);
    }

    @Override
    public PrepareTrackingRecordFormat<RelationshipTypeTokenRecord> relationshipTypeToken() {
        return new PrepareTrackingRecordFormat<>(actual.relationshipTypeToken(), relationshipTypeTokenPrepare);
    }

    @Override
    public PrepareTrackingRecordFormat<DynamicRecord> dynamic() {
        return new PrepareTrackingRecordFormat<>(actual.dynamic(), dynamicPrepare);
    }

    @Override
    public PrepareTrackingRecordFormat<MetaDataRecord> metaData() {
        return new PrepareTrackingRecordFormat<>(actual.metaData(), metaDataPrepare);
    }

    @Override
    public Capability[] capabilities() {
        return actual.capabilities();
    }

    @Override
    public int majorVersion() {
        return actual.majorVersion();
    }

    @Override
    public int minorVersion() {
        return actual.minorVersion();
    }

    @Override
    public boolean hasCapability(Capability capability) {
        return actual.hasCapability(capability);
    }

    @Override
    public FormatFamily getFormatFamily() {
        return FormatFamily.STANDARD;
    }

    @Override
    public boolean hasCompatibleCapabilities(RecordFormats other, CapabilityType type) {
        return actual.hasCompatibleCapabilities(other, type);
    }

    @Override
    public String name() {
        return getClass().getName();
    }

    @Override
    public boolean onlyForMigration() {
        return actual.onlyForMigration();
    }

    @Override
    public StoreFormatLimits idLimits() {
        return actual.idLimits();
    }

    public static class PrepareTrackingRecordFormat<RECORD extends AbstractBaseRecord> implements RecordFormat<RECORD> {
        private final RecordFormat<RECORD> actual;
        private final Set<RECORD> prepare;

        PrepareTrackingRecordFormat(RecordFormat<RECORD> actual, Set<RECORD> prepare) {
            this.actual = actual;
            this.prepare = prepare;
        }

        @Override
        public RECORD newRecord() {
            return actual.newRecord();
        }

        @Override
        public int getRecordSize(StoreHeader storeHeader) {
            return actual.getRecordSize(storeHeader);
        }

        @Override
        public int getRecordHeaderSize() {
            return actual.getRecordHeaderSize();
        }

        @Override
        public boolean isInUse(PageCursor cursor) {
            return actual.isInUse(cursor);
        }

        @Override
        public void read(RECORD record, PageCursor cursor, RecordLoad mode, int recordSize, int recordsPerPage)
                throws IOException {
            actual.read(record, cursor, mode, recordSize, recordsPerPage);
        }

        @Override
        public void prepare(RECORD record, int recordSize, IdSequence idSequence, CursorContext cursorContext) {
            prepare.add(record);
            actual.prepare(record, recordSize, idSequence, cursorContext);
        }

        @Override
        public void write(RECORD record, PageCursor cursor, int recordSize, int recordsPerPage) throws IOException {
            actual.write(record, cursor, recordSize, recordsPerPage);
        }

        @Override
        public long getNextRecordReference(RECORD record) {
            return actual.getNextRecordReference(record);
        }

        @Override
        public long getMaxId() {
            return actual.getMaxId();
        }

        @Override
        public int getFilePageSize(int pageSize, int recordSize) {
            return actual.getFilePageSize(pageSize, recordSize);
        }

        @Override
        public boolean equals(Object otherFormat) {
            return actual.equals(otherFormat);
        }

        @Override
        public int hashCode() {
            return actual.hashCode();
        }

        public boolean prepared(RECORD record) {
            return prepare.contains(record);
        }
    }
}
