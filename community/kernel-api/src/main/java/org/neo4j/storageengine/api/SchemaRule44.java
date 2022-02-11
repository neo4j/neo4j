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
package org.neo4j.storageengine.api;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaUserDescription;

public interface SchemaRule44
{
    record Index(
            long id,
            SchemaDescriptor schema,
            boolean unique,
            String name,
            IndexType indexType,
            IndexProviderDescriptor providerDescriptor,
            IndexConfig indexConfig,
            Long owningConstraintId
    ) implements SchemaRule44
    {
        public String userDescription( TokenNameLookup tokenNameLookup )
        {
            return SchemaUserDescription.forIndex( tokenNameLookup, id, name, unique, indexType.name(), schema, providerDescriptor, owningConstraintId );
        }
    }

    record Constraint(
            long id,
            SchemaDescriptor schema,
            String name,
            ConstraintRuleType constraintRuleType,
            Long ownedIndex,
            IndexType indexType
    ) implements SchemaRule44
    {
    }

    enum IndexType
    {
        BTREE,
        FULLTEXT,
        LOOKUP,
        TEXT,
        RANGE,
        POINT
    }

    enum ConstraintRuleType
    {
        UNIQUE,
        EXISTS,
        UNIQUE_EXISTS
    }

    IndexProviderDescriptor NATIVE_BTREE_10 = new IndexProviderDescriptor( "native-btree", "1.0" );
    IndexProviderDescriptor LUCENE_NATIVE_30 = new IndexProviderDescriptor( "lucene+native", "3.0" );
}
