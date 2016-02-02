/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.store.format.lowlimit;

import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.Capability;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class LowLimitV2_0 extends LowLimitV2_1
{
    public static final RecordFormats RECORD_FORMATS = new LowLimitV2_0();
    public static final String STORE_VERSION = "v0.A.1";

    @Override
    public String storeVersion()
    {
        return STORE_VERSION;
    }

    @Override
    public RecordFormat<NodeRecord> node()
    {
        return new NodeRecordFormatV2_0();
    }

    @Override
    public RecordFormat<RelationshipRecord> relationship()
    {
        return new RelationshipRecordFormatV2_0();
    }

    @Override
    public RecordFormat<RelationshipGroupRecord> relationshipGroup()
    {
        return new NoRecordFormat<>();
    }

    @Override
    public boolean hasStore( StoreType store )
    {
        boolean excluded =
                !super.hasStore( store ) ||
                store == StoreType.RELATIONSHIP_GROUP;
        return !excluded;
    }

    @Override
    public Capability[] capabilities()
    {
        return new Capability[] { Capability.SCHEMA, Capability.LUCENE_3, Capability.VERSION_TRAILERS };
    }
}
