/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.format.aligned;


import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.aligned.Aligned;
import org.neo4j.kernel.impl.store.format.aligned.NodeRecordFormat;
import org.neo4j.kernel.impl.store.format.aligned.RelationshipGroupRecordFormat;
import org.neo4j.kernel.impl.store.format.aligned.RelationshipRecordFormat;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

/**
 * Record format with very high limits, 50-bit per ID, as well the ability to use two record units per record, while at
 * the same time keeping store size small.
 *
 * @see Aligned
 */
public class EnterpriseAligned extends Aligned
{
    public static final RecordFormats RECORD_FORMATS = new EnterpriseAligned();

    @Override
    public String storeVersion()
    {
        // Enterprise.Aligned.Zero
        return "vE.A.0";
    }

    @Override
    public RecordFormat<NodeRecord> node()
    {
        return new NodeRecordFormat( new EnterpriseRecordIO<>() );
    }

    @Override
    public RecordFormat<RelationshipRecord> relationship()
    {
        return new RelationshipRecordFormat( new EnterpriseRecordIO<>() );
    }

    @Override
    public RecordFormat<RelationshipGroupRecord> relationshipGroup()
    {
        return new RelationshipGroupRecordFormat( new EnterpriseRecordIO<>() );
    }
}
