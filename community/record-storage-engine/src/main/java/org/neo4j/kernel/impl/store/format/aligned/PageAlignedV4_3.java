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
package org.neo4j.kernel.impl.store.format.aligned;

import static org.neo4j.kernel.impl.store.format.StoreVersion.ALIGNED_V4_3;

import org.neo4j.kernel.impl.store.format.BaseRecordFormats;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormatFamilyCapability;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.DynamicRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.LabelTokenRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.PropertyKeyTokenRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.PropertyRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipGroupRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipTypeTokenRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.SchemaRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.storageengine.api.format.Index44Compatibility;

/**
 * Record format, very similar to {@link Standard}, only more machine friendly.
 *
 * Pages are padded at the end instead of letting record span 2 pages.
 * As a result, we can ask the OS to fetch and write full 8K pages which
 * it is more happier to work with than for instance 8K - 5 bytes.
 *
 * The only reason why it is just not an evolution of the standard format is
 * that it requires costly migration.
 */
public class PageAlignedV4_3 extends BaseRecordFormats {
    public static final RecordFormats RECORD_FORMATS = new PageAlignedV4_3();
    public static final String NAME = FormatFamily.ALIGNED.name() + "V4_3";

    private PageAlignedV4_3() {
        super(ALIGNED_V4_3, new RecordFormatFamilyCapability(FormatFamily.ALIGNED), Index44Compatibility.INSTANCE);
    }

    @Override
    public RecordFormat<NodeRecord> node() {
        return new NodeRecordFormat(true);
    }

    @Override
    public RecordFormat<RelationshipGroupRecord> relationshipGroup() {
        return new RelationshipGroupRecordFormat(true);
    }

    @Override
    public RecordFormat<RelationshipRecord> relationship() {
        return new RelationshipRecordFormat(true);
    }

    @Override
    public RecordFormat<PropertyRecord> property() {
        return new PropertyRecordFormat(true);
    }

    @Override
    public RecordFormat<LabelTokenRecord> labelToken() {
        return new LabelTokenRecordFormat(true);
    }

    @Override
    public RecordFormat<PropertyKeyTokenRecord> propertyKeyToken() {
        return new PropertyKeyTokenRecordFormat(true);
    }

    @Override
    public RecordFormat<RelationshipTypeTokenRecord> relationshipTypeToken() {
        return new RelationshipTypeTokenRecordFormat(true);
    }

    @Override
    public RecordFormat<DynamicRecord> dynamic() {
        return new DynamicRecordFormat(true);
    }

    @Override
    public RecordFormat<SchemaRecord> schema() {
        return new SchemaRecordFormat(true);
    }

    @Override
    public FormatFamily getFormatFamily() {
        return FormatFamily.ALIGNED;
    }

    @Override
    public String name() {
        return NAME;
    }
}
