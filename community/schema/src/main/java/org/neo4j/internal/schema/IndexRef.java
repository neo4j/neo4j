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

public interface IndexRef<T extends IndexRef<T>> extends SchemaDescriptorSupplier {
    /**
     * Returns true if this index is only meant to allow one value per key.
     */
    boolean isUnique();

    /**
     * Returns the {@link IndexType} of this index.
     */
    IndexType getIndexType();

    /**
     * Returns the {@link IndexProviderDescriptor} of the index provider for this index.
     */
    IndexProviderDescriptor getIndexProvider();

    /**
     * Produce a new index reference that is the same as this index reference in every way, except it has the given index provider descriptor.
     *
     * @param indexProvider The index provider descriptor used in the new index reference.
     * @return A new index reference with the given index provider.
     */
    T withIndexProvider(IndexProviderDescriptor indexProvider);

    /**
     * Produce a new index reference that is the same as this index reference in every way, except it has the given schema descriptor.
     *
     * @param schema The schema descriptor used in the new index reference.
     * @return A new index reference with the given schema descriptor.
     */
    T withSchemaDescriptor(SchemaDescriptor schema);

    /**
     * @return the attached {@link IndexConfig}.
     */
    IndexConfig getIndexConfig();

    /**
     * Produce a new index reference that is the same as this index reference in every way, except it has the given index config.
     * @param indexConfig The index config of the new index reference.
     * @return A new index reference with the given index config.
     */
    T withIndexConfig(IndexConfig indexConfig);

    /**
     * @return true if this {@link IndexRef index reference} is a token index, otherwise false.
     */
    default boolean isTokenIndex() {
        return schema().isSchemaDescriptorType(AnyTokenSchemaDescriptor.class) && getIndexType() == IndexType.LOOKUP;
    }
}
