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
package org.neo4j.kernel.api.impl.schema;

import static org.neo4j.internal.schema.IndexType.RANGE;
import static org.neo4j.kernel.impl.index.schema.IndexEntryTestUtil.generateStringResultingInIndexEntrySize;

import org.neo4j.index.internal.gbptree.DynamicSizeUtil;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.test.RandomSupport;

public class RangeIndexStringLengthIndexValidationIT extends StringLengthIndexValidationIT {
    @Override
    protected int getSingleKeySizeLimit(int payloadSize) {
        return DynamicSizeUtil.keyValueSizeCapFromPageSize(payloadSize);
    }

    @Override
    protected String getString(RandomSupport random, int keySize) {
        return generateStringResultingInIndexEntrySize(keySize);
    }

    @Override
    protected IndexType getIndexType() {
        return RANGE;
    }

    @Override
    protected IndexProviderDescriptor getIndexProvider() {
        return AllIndexProviderDescriptors.RANGE_DESCRIPTOR;
    }

    @Override
    protected String expectedPopulationFailureCauseMessage(long indexId, long entityId) {
        return String.format(
                "Property value is too large to index, please see index documentation for limitations. "
                        + "Index: Index( id=%d, name='coolName', type='RANGE', "
                        + "schema=(:LABEL_ONE {largeString}), indexProvider='range-1.0' ), entity id: %d",
                indexId, entityId);
    }
}
