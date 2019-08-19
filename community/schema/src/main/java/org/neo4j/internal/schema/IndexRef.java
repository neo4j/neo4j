/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.schema;

interface IndexRef<T extends IndexRef<T>> extends SchemaDescriptorSupplier
{
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
    T withIndexProvider( IndexProviderDescriptor indexProvider );

    /**
     * Produce a new index reference that is the same as this index reference in every way, except it has the given schema descriptor.
     *
     * @param schema The schema descriptor used in the new index reference.
     * @return A new index reference with the given schema descriptor.
     */
    T withSchemaDescriptor( SchemaDescriptor schema );

    /**
     * Compute the structural equivalence between the two index references.
     *
     * Two index references are structurally equivalent if their schemas are equal, and they have the same uniqueness setting.
     *
     * @param a the first reference.
     * @param b the other reference.
     * @return {@code true} if the two references are structurally equivalent.
     */
    static boolean equals( IndexRef<?> a, IndexRef<?> b )
    {
        return a.isUnique() == b.isUnique() && a.schema().equals( b.schema() );
    }

    /**
     * Compute the structural hash code of this index reference.
     *
     * The structural hash code is based on the uniqueness setting, and the schema of the index reference.
     *
     * @param ref the index reference to compute the hash code for.
     * @return the hash code of the given index reference.
     */
    static int hashCode( IndexRef<?> ref )
    {
        int result = 1;
        result = 31 * result + Boolean.hashCode( ref.isUnique() );
        result = 31 * result + ref.schema().hashCode();
        return result;
    }
}
