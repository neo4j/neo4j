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

import static org.neo4j.internal.schema.IndexType.TEXT;

import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.impl.api.LuceneIndexValueValidator;
import org.neo4j.test.RandomSupport;

public class TrigramIndexStringLengthIndexValidationIT extends StringLengthIndexValidationIT {

    @Override
    protected int getSingleKeySizeLimit(int payloadSize) {
        return LuceneIndexValueValidator.MAX_TERM_LENGTH;
    }

    @Override
    protected String getString(RandomSupport random, int keySize) {
        return random.nextAlphaNumericString(keySize, keySize);
    }

    @Override
    protected IndexType getIndexType() {
        return TEXT;
    }

    @Override
    protected IndexProviderDescriptor getIndexProvider() {
        return AllIndexProviderDescriptors.TEXT_V2_DESCRIPTOR;
    }

    @Override
    protected String expectedPopulationFailureCauseMessage(long indexId, long entityId) {
        return String.format(
                "Property value is too large to index, please see index documentation for limitations. "
                        + "Index: Index( id=%d, name='coolName', type='TEXT', "
                        + "schema=(:LABEL_ONE {largeString}), indexProvider='text-2.0' ), entity id: %d",
                indexId, entityId);
    }
}
