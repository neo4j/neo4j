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
package org.neo4j.internal.schema;

import static java.util.Objects.requireNonNull;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_ID_NAME_LOOKUP;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Stream;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.string.Mask;

public final class IndexDescriptor implements IndexRef<IndexDescriptor>, SchemaRule {
    /**
     * A special index descriptor used to represent the absence of an index.
     * This descriptor <em>cannot</em> be modified by any of the {@code with*} methods.
     */
    public static final IndexDescriptor NO_INDEX = new IndexDescriptor();
    // Needs to be possible to create IndexDescriptors with this special id during migration
    public static final long FORMER_LABEL_SCAN_STORE_ID = -2;

    private final long id;
    private final String name;
    private final SchemaDescriptor schema;
    private final boolean isUnique;
    private final IndexProviderDescriptor indexProvider;
    private final Long owningConstraintId;
    private final IndexCapability capability;
    private final IndexType indexType;
    private final IndexConfig indexConfig;

    IndexDescriptor(long id, IndexPrototype prototype) {
        this(
                id,
                SchemaNameUtil.sanitiseName(prototype.getName()),
                prototype.schema(),
                prototype.isUnique(),
                prototype.getIndexProvider(),
                null,
                IndexCapability.NO_CAPABILITY,
                prototype.getIndexType(),
                prototype.getIndexConfig());
    }

    private IndexDescriptor(
            long id,
            String name,
            SchemaDescriptor schema,
            boolean isUnique,
            IndexProviderDescriptor indexProvider,
            Long owningConstraintId,
            IndexCapability capability,
            IndexType indexType,
            IndexConfig indexConfig) {
        if (id < 0 && id != FORMER_LABEL_SCAN_STORE_ID) {
            throw new IllegalArgumentException(
                    "The id of an index must not be negative, but it was attempted to assign " + id + ".");
        }
        name = SchemaNameUtil.sanitiseName(name);
        requireNonNull(schema, "The schema of an index cannot be null.");
        requireNonNull(indexProvider, "The index provider cannot be null.");
        // The 'owningConstraintId' is allowed to be null, which is the case when an index descriptor is initially
        // created.
        requireNonNull(capability, "The index capability cannot be null.");
        requireNonNull(indexConfig, "The index configuration cannot be null.");

        this.id = id;
        this.name = name;
        this.schema = schema;
        this.isUnique = isUnique;
        this.indexProvider = indexProvider;
        this.owningConstraintId = owningConstraintId;
        this.capability = capability;
        this.indexType = indexType;
        this.indexConfig = indexConfig;
    }

    /**
     * This constructor is used <em>exclusively</em> for the {@link #NO_INDEX} field!
     */
    private IndexDescriptor() {
        this.id = -1;
        this.name = ReservedSchemaRuleNames.NO_INDEX.getReservedName();
        this.schema = SchemaDescriptors.noSchema();
        this.isUnique = false;
        this.indexProvider = IndexProviderDescriptor.UNDECIDED;
        this.owningConstraintId = null;
        this.capability = IndexCapability.NO_CAPABILITY;
        this.indexType = IndexType.RANGE;
        this.indexConfig = IndexConfig.empty();
    }

    @Override
    public SchemaDescriptor schema() {
        return schema;
    }

    @Override
    public boolean isUnique() {
        return isUnique;
    }

    @Override
    public long getId() {
        return id;
    }

    /**
     * @return The name of this index.
     */
    @Override
    public String getName() {
        return name;
    }

    @Override
    public IndexDescriptor withName(String name) {
        if (name == null) {
            return this;
        }
        name = SchemaNameUtil.sanitiseName(name);
        return new IndexDescriptor(
                id, name, schema, isUnique, indexProvider, owningConstraintId, capability, indexType, indexConfig);
    }

    /**
     * @return the {@link IndexConfig}
     */
    @Override
    public IndexConfig getIndexConfig() {
        return indexConfig;
    }

    /**
     * Produce a new index descriptor that is the same as this index descriptor in every way, except it has the given index config.
     * @param indexConfig The index config of the new index descriptor.
     * @return A new index descriptor with the given index config.
     */
    @Override
    public IndexDescriptor withIndexConfig(IndexConfig indexConfig) {
        return new IndexDescriptor(
                id, name, schema, isUnique, indexProvider, owningConstraintId, capability, indexType, indexConfig);
    }

    /**
     * @return The id of the constraint that owns this index, if such a constraint exists. Otherwise {@code empty}.
     */
    public OptionalLong getOwningConstraintId() {
        return owningConstraintId == null ? OptionalLong.empty() : OptionalLong.of(owningConstraintId);
    }

    @Override
    public String userDescription(TokenNameLookup tokenNameLookup) {
        return userDescription(tokenNameLookup, Mask.NO);
    }

    private String userDescription(TokenNameLookup tokenNameLookup, Mask mask) {
        return SchemaUserDescription.forIndex(
                tokenNameLookup, id, name, indexType.name(), schema(), getIndexProvider(), owningConstraintId, mask);
    }

    @Override
    public IndexType getIndexType() {
        return indexType;
    }

    @Override
    public IndexProviderDescriptor getIndexProvider() {
        return indexProvider;
    }

    /**
     * Return the capabilities of this index.
     */
    public IndexCapability getCapability() {
        return capability;
    }

    @Override
    public IndexDescriptor withIndexProvider(IndexProviderDescriptor indexProvider) {
        return new IndexDescriptor(
                id, name, schema, isUnique, indexProvider, owningConstraintId, capability, indexType, indexConfig);
    }

    @Override
    public IndexDescriptor withSchemaDescriptor(SchemaDescriptor schema) {
        return new IndexDescriptor(
                id, name, schema, isUnique, indexProvider, owningConstraintId, capability, indexType, indexConfig);
    }

    /**
     * Produce a new index descriptor that is the same as this index descriptor in every way, except it has the given owning constraint id.
     *
     * @param owningConstraintId The id of the constraint that owns the index represented by this index descriptor.
     * @return A new index descriptor with the given owning constraint id.
     */
    public IndexDescriptor withOwningConstraintId(long owningConstraintId) {
        if (!isUnique()) {
            throw new IllegalStateException("Cannot assign an owning constraint id (in this case " + owningConstraintId
                    + ") to a non-unique index: " + this + ".");
        }
        if (owningConstraintId < 0) {
            throw new IllegalArgumentException(
                    "The owning constraint id of an index must not be negative, but it was attempted to assign "
                            + owningConstraintId + ".");
        }
        return new IndexDescriptor(
                id, name, schema, isUnique, indexProvider, owningConstraintId, capability, indexType, indexConfig);
    }

    /**
     * Produce a new index descriptor that is the same as this index descriptor in every way, except it has the given index capabilities.
     *
     * @param capability The capabilities of the new index descriptor.
     * @return A new index descriptor with the given capabilities.
     */
    public IndexDescriptor withIndexCapability(IndexCapability capability) {
        return new IndexDescriptor(
                id, name, schema, isUnique, indexProvider, owningConstraintId, capability, indexType, indexConfig);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexDescriptor that = (IndexDescriptor) o;

        if (id != that.id) {
            return false;
        }
        if (isUnique != that.isUnique) {
            return false;
        }
        if (indexType != that.indexType) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (!schema.equals(that.schema)) {
            return false;
        }
        return indexProvider.equals(that.indexProvider);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public String toString() {
        return toString(Mask.NO);
    }

    @Override
    public String toString(Mask mask) {
        // TOKEN_ID_NAME_LOOKUP makes sure we don't include schema token names, regardless of masking
        return userDescription(TOKEN_ID_NAME_LOOKUP, mask);
    }

    /**
     * Sorts indexes by type, returning first GENERAL indexes, followed by UNIQUE. Implementation is not suitable in hot path.
     *
     * @param indexes Indexes to sort
     * @return sorted indexes
     */
    public static Iterator<IndexDescriptor> sortByType(Iterator<IndexDescriptor> indexes) {
        List<IndexDescriptor> nonUnique = new ArrayList<>();
        List<IndexDescriptor> unique = new ArrayList<>();
        indexes.forEachRemaining(index -> (index.isUnique() ? unique : nonUnique).add(index));
        return Stream.concat(nonUnique.stream(), unique.stream()).iterator();
    }
}
