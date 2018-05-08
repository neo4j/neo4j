/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.schema.index;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.values.storable.ValueCategory;

import static java.lang.String.format;
import static org.neo4j.kernel.api.schema.index.PendingIndexDescriptor.Filter.GENERAL;
import static org.neo4j.kernel.api.schema.index.PendingIndexDescriptor.Filter.UNIQUE;

/**
 * Internal representation of a graph index, including the schema unit it targets (eg. label-property combination)
 * and the type of index. UNIQUE indexes are used to back uniqueness constraints.
 */
public class PendingIndexDescriptor implements SchemaDescriptorSupplier, CapableIndexReference
{
    public enum Type
    {
        GENERAL,
        UNIQUE
    }

    public enum Filter implements Predicate<PendingIndexDescriptor>
    {
        GENERAL
                {
                    @Override
                    public boolean test( PendingIndexDescriptor index )
                    {
                        return index.type == Type.GENERAL;
                    }
                },
        UNIQUE
                {
                    @Override
                    public boolean test( PendingIndexDescriptor index )
                    {
                        return index.type == Type.UNIQUE;
                    }
                },
        ANY
                {
                    @Override
                    public boolean test( PendingIndexDescriptor index )
                    {
                        return true;
                    }
                }
    }

    public interface Supplier
    {
        PendingIndexDescriptor getIndexDescriptor();
    }

    protected final SchemaDescriptor schema;
    protected final PendingIndexDescriptor.Type type;
    protected final Optional<String> name;
    protected final IndexProvider.Descriptor providerDescriptor;

    public PendingIndexDescriptor( SchemaDescriptor schema,
                                   Type type,
                                   Optional<String> name,
                                   IndexProvider.Descriptor providerDescriptor )
    {
        this.schema = schema;
        this.type = type;
        this.name = name;
        this.providerDescriptor = providerDescriptor;
    }

    // METHODS

    public Type type()
    {
        return type;
    }

    /**
     * This method currently returns the specific LabelSchemaDescriptor, as we do not support indexes on relations.
     * When we do, consider down-typing this to a SchemaDescriptor.
     */
    @Override
    public SchemaDescriptor schema()
    {
        return schema;
    }

    @Override
    public boolean isUnique()
    {
        return type == Type.UNIQUE;
    }

    @Override
    public int label()
    {
        return schema.keyId();
    }

    @Override
    public int[] properties()
    {
        return schema.getPropertyIds();
    }

    @Override
    public String providerKey()
    {
        return providerDescriptor.getKey();
    }

    @Override
    public String providerVersion()
    {
        return providerDescriptor.getVersion();
    }

    public Optional<String> name()
    {
        return name;
    }

    public IndexProvider.Descriptor providerDescriptor()
    {
        return providerDescriptor;
    }

    @Override
    public IndexOrder[] orderCapability( ValueCategory... valueCategories )
    {
        return ORDER_NONE;
    }

    @Override
    public IndexValueCapability valueCapability( ValueCategory... valueCategories )
    {
        return IndexValueCapability.NO;
    }

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of what this index indexes.
     */
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return format( "Index( %s, %s )", type.name(), schema.userDescription( tokenNameLookup ) );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o instanceof PendingIndexDescriptor )
        {
            PendingIndexDescriptor that = (PendingIndexDescriptor)o;
            return this.type() == that.type() && this.schema().equals( that.schema() );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return type.hashCode() & schema.hashCode();
    }

    @Override
    public String toString()
    {
        return userDescription( SchemaUtil.idTokenNameLookup );
    }

    /**
     * Sorts indexes by type, returning first GENERAL indexes, followed by UNIQUE. Implementation is not suitable in
     * hot path.
     *
     * @param indexes Indexes to sort
     * @return sorted indexes
     */
    public static Iterator<PendingIndexDescriptor> sortByType( Iterator<PendingIndexDescriptor> indexes )
    {
        List<PendingIndexDescriptor> materialized = Iterators.asList( indexes );
        return Iterators.concat(
                Iterators.filter( GENERAL, materialized.iterator() ),
                Iterators.filter( UNIQUE, materialized.iterator() ) );
    }
}
