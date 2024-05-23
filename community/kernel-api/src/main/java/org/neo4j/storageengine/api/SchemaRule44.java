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
package org.neo4j.storageengine.api;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaUserDescription;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.util.Preconditions;

public interface SchemaRule44 {
    long id();

    String userDescription(TokenNameLookup tokenNameLookup);

    SchemaRule convertTo50rule();

    record Index(
            long id,
            SchemaDescriptor schema,
            boolean unique,
            String name,
            IndexType indexType,
            IndexProviderDescriptor providerDescriptor,
            IndexConfig indexConfig,
            Long owningConstraintId)
            implements SchemaRule44 {
        @Override
        public String userDescription(TokenNameLookup tokenNameLookup) {
            return SchemaUserDescription.forIndex(
                    tokenNameLookup, id, name, indexType.name(), schema, providerDescriptor, owningConstraintId);
        }

        @Override
        public SchemaRule convertTo50rule() {
            Preconditions.checkState(
                    indexType != IndexType.BTREE,
                    "Unsupported migration for schema rule with BTREE index type. Id: " + id);

            IndexPrototype prototype =
                    unique ? IndexPrototype.uniqueForSchema(schema) : IndexPrototype.forSchema(schema);
            prototype = prototype
                    .withName(name)
                    .withIndexType(indexType.convertIndexType())
                    .withIndexProvider(providerDescriptor);

            IndexDescriptor index = prototype.materialise(id);

            index = index.withIndexConfig(indexConfig);

            if (owningConstraintId != null) {
                index = index.withOwningConstraintId(owningConstraintId);
            }

            return index;
        }
    }

    record Constraint(
            long id,
            SchemaDescriptor schema,
            String name,
            ConstraintRuleType constraintRuleType,
            Long ownedIndex,
            IndexType indexType)
            implements SchemaRule44 {
        @Override
        public String userDescription(TokenNameLookup tokenNameLookup) {
            return SchemaUserDescription.forConstraint(
                    tokenNameLookup, id, name, constraintRuleType.asConstraintType(), schema, ownedIndex, null);
        }

        @Override
        public SchemaRule convertTo50rule() {
            ConstraintDescriptor constraint;
            switch (constraintRuleType) {
                case UNIQUE -> {
                    Preconditions.checkState(
                            indexType == IndexType.RANGE,
                            "Unsupported migration for constraint schema rule backed by BTREE index type. Id: " + id);
                    constraint = ConstraintDescriptorFactory.uniqueForSchema(schema, indexType.convertIndexType());
                    if (ownedIndex != null) {
                        constraint = constraint.withOwnedIndexId(ownedIndex);
                    }
                }
                case EXISTS -> constraint = ConstraintDescriptorFactory.existsForSchema(schema, false);
                case UNIQUE_EXISTS -> {
                    Preconditions.checkState(
                            indexType == IndexType.RANGE,
                            "Unsupported migration for constraint schema rule backed by BTREE index type. Id: " + id);
                    constraint = ConstraintDescriptorFactory.keyForSchema(schema, indexType.convertIndexType());
                    if (ownedIndex != null) {
                        constraint = constraint.withOwnedIndexId(ownedIndex);
                    }
                }
                default -> throw new IllegalStateException(
                        "Unsupported migration for constraint of type " + constraintRuleType.name());
            }
            return constraint.withId(id).withName(name);
        }
    }

    enum IndexType {
        BTREE,
        FULLTEXT,
        LOOKUP,
        TEXT,
        RANGE,
        POINT;

        org.neo4j.internal.schema.IndexType convertIndexType() {
            return switch (this) {
                case BTREE -> throw new IllegalStateException("Trying to convert unsupported index type 'BTREE'");
                case FULLTEXT -> org.neo4j.internal.schema.IndexType.FULLTEXT;
                case LOOKUP -> org.neo4j.internal.schema.IndexType.LOOKUP;
                case TEXT -> org.neo4j.internal.schema.IndexType.TEXT;
                case RANGE -> org.neo4j.internal.schema.IndexType.RANGE;
                case POINT -> org.neo4j.internal.schema.IndexType.POINT;
            };
        }
    }

    enum ConstraintRuleType {
        UNIQUE(true, ConstraintType.UNIQUE),
        EXISTS(false, ConstraintType.EXISTS),
        UNIQUE_EXISTS(true, ConstraintType.UNIQUE_EXISTS);

        private final boolean isIndexBacked;
        private final ConstraintType constraintType;

        ConstraintRuleType(boolean isIndexBacked, ConstraintType constraintType) {
            this.isIndexBacked = isIndexBacked;
            this.constraintType = constraintType;
        }

        public boolean isIndexBacked() {
            return isIndexBacked;
        }

        public ConstraintType asConstraintType() {
            return constraintType;
        }
    }

    IndexProviderDescriptor NATIVE_BTREE_10 = new IndexProviderDescriptor("native-btree", "1.0");
    IndexProviderDescriptor LUCENE_NATIVE_30 = new IndexProviderDescriptor("lucene+native", "3.0");
}
