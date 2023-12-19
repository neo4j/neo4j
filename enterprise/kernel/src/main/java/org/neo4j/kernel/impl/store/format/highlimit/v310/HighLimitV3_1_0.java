/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.store.format.highlimit.v310;

import org.neo4j.kernel.impl.store.format.BaseRecordFormats;
import org.neo4j.kernel.impl.store.format.Capability;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.store.format.highlimit.DynamicRecordFormat;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimitFormatFamily;
import org.neo4j.kernel.impl.store.format.standard.LabelTokenRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.PropertyKeyTokenRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipTypeTokenRecordFormat;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

/**
 * Record format with very high limits, 50-bit per ID, while at the same time keeping store size small.
 *
 * @see BaseHighLimitRecordFormatV3_1_0
 */
public class HighLimitV3_1_0 extends BaseRecordFormats
{
    /**
     * Default maximum number of bits that can be used to represent id
     */
    static final int DEFAULT_MAXIMUM_BITS_PER_ID = 50;

    public static final String STORE_VERSION = StoreVersion.HIGH_LIMIT_V3_1_0.versionString();

    public static final RecordFormats RECORD_FORMATS = new HighLimitV3_1_0();
    public static final String NAME = "high_limitV3_1_0";

    public HighLimitV3_1_0()
    {
        super( STORE_VERSION, StoreVersion.HIGH_LIMIT_V3_1_0.introductionVersion(), 3, Capability.DENSE_NODES,
                Capability.RELATIONSHIP_TYPE_3BYTES, Capability.SCHEMA, Capability.LUCENE_5, Capability.SECONDARY_RECORD_UNITS );
    }

    @Override
    public RecordFormat<NodeRecord> node()
    {
        return new NodeRecordFormatV3_1_0();
    }

    @Override
    public RecordFormat<RelationshipRecord> relationship()
    {
        return new RelationshipRecordFormatV3_1_0();
    }

    @Override
    public RecordFormat<RelationshipGroupRecord> relationshipGroup()
    {
        return new RelationshipGroupRecordFormatV3_1_0();
    }

    @Override
    public RecordFormat<PropertyRecord> property()
    {
        return new PropertyRecordFormatV3_1_0();
    }

    @Override
    public RecordFormat<LabelTokenRecord> labelToken()
    {
        return new LabelTokenRecordFormat();
    }

    @Override
    public RecordFormat<PropertyKeyTokenRecord> propertyKeyToken()
    {
        return new PropertyKeyTokenRecordFormat();
    }

    @Override
    public RecordFormat<RelationshipTypeTokenRecord> relationshipTypeToken()
    {
        return new RelationshipTypeTokenRecordFormat();
    }

    @Override
    public RecordFormat<DynamicRecord> dynamic()
    {
        return new DynamicRecordFormat();
    }

    @Override
    public FormatFamily getFormatFamily()
    {
        return HighLimitFormatFamily.INSTANCE;
    }

    @Override
    public String name()
    {
        return NAME;
    }
}
