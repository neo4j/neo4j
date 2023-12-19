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
package org.neo4j.kernel.impl.storemigration;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.LongSupplier;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.schema.AnyTokenSchemaDescriptor;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.storageengine.api.SchemaRule44;
import org.neo4j.token.TokenHolders;

public class SchemaStore44MigrationUtil {

    public static final AnyTokenSchemaDescriptor FORMER_LABEL_SCAN_STORE_SCHEMA =
            SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR;

    public static final String FORMER_LABEL_SCAN_STORE_GENERATED_NAME =
            "__org_neo4j_schema_index_label_scan_store_converted_to_token_index";

    static SchemaInfo44 extractRuleInfo(boolean shouldCreateNewSchemaStore, List<SchemaRule44> all) {
        var toDelete = new ArrayList<SchemaRule44>();
        var toCreate = new ArrayList<SchemaRule>();

        // Organize indexes by SchemaDescriptor
        var indexesBySchema = new HashMap<SchemaDescriptor, List<SchemaRule44.Index>>();
        var uniqueIndexesByName = new HashMap<String, SchemaRule44.Index>();
        var constraintBySchemaAndType = new HashMap<
                SchemaDescriptor, EnumMap<SchemaRule44.ConstraintRuleType, List<SchemaRule44.Constraint>>>();
        for (var schemaRule : all) {
            if (schemaRule instanceof SchemaRule44.Index index) {
                if (!index.unique()) {
                    indexesBySchema
                            .computeIfAbsent(index.schema(), k -> new ArrayList<>())
                            .add(index);
                } else {
                    uniqueIndexesByName.put(index.name(), index);
                }

                if (shouldCreateNewSchemaStore && index.indexType() != SchemaRule44.IndexType.BTREE) {
                    toCreate.add(schemaRule.convertTo50rule());
                }
            }
            if (schemaRule instanceof SchemaRule44.Constraint constraint) {
                boolean indexBacked = constraint.constraintRuleType().isIndexBacked();
                if (indexBacked) {
                    var constraintsByType = constraintBySchemaAndType.computeIfAbsent(
                            constraint.schema(), k -> new EnumMap<>(SchemaRule44.ConstraintRuleType.class));
                    constraintsByType
                            .computeIfAbsent(constraint.constraintRuleType(), k -> new ArrayList<>())
                            .add(constraint);
                }

                if (shouldCreateNewSchemaStore
                        && (!indexBacked || constraint.indexType() == SchemaRule44.IndexType.RANGE)) {
                    toCreate.add(schemaRule.convertTo50rule());
                }
            }
        }

        // Make sure node label index is correctly persisted. There are two situations where it might not be:
        // 1. As part of upgrade to 4.3/4.4 a schema record without any properties was written to the schema store.
        //    This record was used to represent the old label scan store (< 4.3) converted to node label index.
        //    In this case we need to rewrite this schema to give it the properties it should have. In this way we can
        // keep the index id.
        // 2. If no write transaction happened after upgrade of old store to 4.3/4.4 the upgrade transaction was never
        // injected
        //    and node label index (as schema rule with no properties) was never persisted at all. In this case
        //    IndexDescriptor#INJECTED_NLI will be injected by SchemaStore44Reader
        //    when reading schema rules. In this case we materialise this injected rule with a new real id (instead of
        // -2).
        // The ids are selected in migrate, here we just make sure that the rule will always be added later
        List<SchemaRule44.Index> nlis = indexesBySchema.get(FORMER_LABEL_SCAN_STORE_SCHEMA);
        if (!shouldCreateNewSchemaStore && nlis != null && !nlis.isEmpty()) {
            SchemaRule44.Index nli = nlis.get(0);
            if (FORMER_LABEL_SCAN_STORE_GENERATED_NAME.equals(nli.name())) {
                toCreate.add(nli.convertTo50rule());
            }
        }

        // Figure out which btree indexes that has replacement and can be deleted and which don't
        var nonReplacedIndexes = new ArrayList<SchemaRule44.Index>();
        for (var schema : indexesBySchema.keySet()) {
            List<SchemaRule44.Index> indexes = indexesBySchema.get(schema);
            for (SchemaRule44.Index index : indexes) {
                if (index.indexType() == SchemaRule44.IndexType.BTREE) {
                    if (indexes.size() == 1) {
                        nonReplacedIndexes.add(index);
                    } else {
                        toDelete.add(index);
                    }
                }
            }
        }

        // Figure out which constraints, backed by btree indexes, that has replacement and can be deleted and which
        // don't
        var nonReplacedConstraints = new ArrayList<Pair<SchemaRule44.Constraint, SchemaRule44.Index>>();
        constraintBySchemaAndType.values().stream()
                .flatMap(enumMap -> enumMap.values().stream())
                .forEach(constraintsGroupedBySchemaAndType -> {
                    for (var constraint : constraintsGroupedBySchemaAndType) {
                        var backingIndex = uniqueIndexesByName.remove(constraint.name());
                        if (backingIndex.indexType() == SchemaRule44.IndexType.BTREE) {
                            if (constraintsGroupedBySchemaAndType.size() == 1) {
                                nonReplacedConstraints.add(Pair.of(constraint, backingIndex));
                            } else {
                                toDelete.add(constraint);
                                toDelete.add(backingIndex);
                            }
                        }
                    }
                });

        for (SchemaRule44.Index uniqueIndex : uniqueIndexesByName.values()) {
            // There could be a unique index not linked to a constraint e.g. by crashing in the middle of a constraint
            // creation,
            // since we don't know what constraint type it should be backing we can't know if it is replaced - let's
            // just throw on it instead.
            if (uniqueIndex.indexType() == SchemaRule44.IndexType.BTREE) {
                nonReplacedIndexes.add(uniqueIndex);
            }
        }
        return new SchemaInfo44(toDelete, toCreate, nonReplacedIndexes, nonReplacedConstraints);
    }

    static void assertCanMigrate(
            boolean forceBtreeIndexesToRange,
            List<SchemaRule44.Index> nonReplacedIndexes,
            List<Pair<SchemaRule44.Constraint, SchemaRule44.Index>> nonReplacedConstraints,
            TokenHolders srcTokenHolders) {
        if (!forceBtreeIndexesToRange && (!nonReplacedIndexes.isEmpty() || !nonReplacedConstraints.isEmpty())) {
            // Throw if non-replaced index exists
            var nonReplacedIndexString = new StringJoiner(", ", "[", "]");
            var nonReplacedConstraintsString = new StringJoiner(", ", "[", "]");
            nonReplacedIndexes.forEach(index -> nonReplacedIndexString.add(index.userDescription(srcTokenHolders)));
            nonReplacedConstraints.forEach(
                    pair -> nonReplacedConstraintsString.add(pair.first().userDescription(srcTokenHolders)));
            throw new IllegalStateException(
                    "Migration will remove all BTREE indexes and constraints backed by BTREE indexes. "
                            + "To guard against unintentionally removing indexes or constraints, "
                            + "it is recommended for all BTREE indexes or constraints backed by BTREE indexes to have a valid replacement. "
                            + "Indexes can be replaced by RANGE, TEXT or POINT index and constraints can be replaced by constraints backed by RANGE index. "
                            + "Please drop your indexes and constraints or create replacements and retry the migration. "
                            + "The indexes and constraints without replacement are: "
                            + nonReplacedIndexString + " and " + nonReplacedConstraintsString + ". "
                            + "Alternatively, you can use the option --force-btree-indexes-to-range to force all BTREE indexes or constraints backed by "
                            + "BTREE indexes to be replaced by RANGE equivalents. Be aware that RANGE indexes are not always the optimal replacement of BTREEs "
                            + "and performance may be affected while the new indexes are populated. See the Neo4j v5 migration guide online for more information.");
        }
    }

    static ConstraintDescriptor asRangeBackedConstraint(
            SchemaRule44.Constraint constraint,
            IndexDescriptor rangeIndex,
            LongSupplier idSupplier,
            TokenHolders dstTokensHolders) {
        ConstraintDescriptor newConstraint;
        if (constraint.constraintRuleType() == SchemaRule44.ConstraintRuleType.UNIQUE) {
            newConstraint = ConstraintDescriptorFactory.uniqueForSchema(constraint.schema(), rangeIndex.getIndexType());
        } else if (constraint.constraintRuleType() == SchemaRule44.ConstraintRuleType.UNIQUE_EXISTS) {
            newConstraint = ConstraintDescriptorFactory.keyForSchema(constraint.schema(), rangeIndex.getIndexType());
        } else {
            throw new IllegalStateException("We should never see non-index-backed constraint here, but got: "
                    + constraint.userDescription(dstTokensHolders));
        }
        return newConstraint
                .withOwnedIndexId(rangeIndex.getId())
                .withName(constraint.name())
                .withId(idSupplier.getAsLong());
    }

    static IndexDescriptor asRangeIndex(SchemaRule44.Index btreeIndex, LongSupplier idSupplier) {
        var prototype = btreeIndex.unique()
                ? IndexPrototype.uniqueForSchema(btreeIndex.schema())
                : IndexPrototype.forSchema(btreeIndex.schema());
        return prototype
                .withName(btreeIndex.name())
                .withIndexType(IndexType.RANGE)
                .withIndexProvider(new IndexProviderDescriptor("range", "1.0"))
                .materialise(idSupplier.getAsLong());
    }

    record SchemaInfo44(
            ArrayList<SchemaRule44> toDelete,
            ArrayList<SchemaRule> toCreate,
            ArrayList<SchemaRule44.Index> nonReplacedIndexes,
            ArrayList<Pair<SchemaRule44.Constraint, SchemaRule44.Index>> nonReplacedConstraints) {}
}
