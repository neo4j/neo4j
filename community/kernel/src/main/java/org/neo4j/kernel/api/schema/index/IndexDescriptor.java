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

import java.util.Optional;
import java.util.function.Predicate;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.values.storable.ValueCategory;

import static java.lang.String.format;

/**
 * Internal representation of a graph index, including the schema unit it targets (eg. label-property combination)
 * and the type of index. UNIQUE indexes are used to back uniqueness constraints.
 */
public class IndexDescriptor implements SchemaDescriptorSupplier, IndexReference
{
    protected final SchemaDescriptor schema;
    protected final IndexDescriptor.Type type;
    protected final Optional<String> userSuppliedName;
    protected final IndexProvider.Descriptor providerDescriptor;

    IndexDescriptor( IndexDescriptor indexDescriptor )
    {
        schema = indexDescriptor.schema;
        type = indexDescriptor.type;
        userSuppliedName =  indexDescriptor.userSuppliedName;
        providerDescriptor = indexDescriptor.providerDescriptor;
    }

    public enum Type
    {
        GENERAL,
        UNIQUE

    }
    public enum Filter implements Predicate<IndexDescriptor>
    {
        GENERAL
                {
                    @Override
                    public boolean test( IndexDescriptor index )
                    {
                        return index.type == Type.GENERAL;
                    }
                },
        UNIQUE
                {
                    @Override
                    public boolean test( IndexDescriptor index )
                    {
                        return index.type == Type.UNIQUE;
                    }
                },
        ANY
                {
                    @Override
                    public boolean test( IndexDescriptor index )
                    {
                        return true;
                    }
                }

    }
    public interface Supplier
    {
        IndexDescriptor getIndexDescriptor();

    }

    public IndexDescriptor( SchemaDescriptor schema,
                            Type type,
                            Optional<String> userSuppliedName,
                            IndexProvider.Descriptor providerDescriptor )
    {
        this.schema = schema;
        this.type = type;
        this.userSuppliedName = userSuppliedName;
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
        if ( o instanceof IndexDescriptor )
        {
            IndexDescriptor that = (IndexDescriptor)o;
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

    public StoreIndexDescriptor withId( long id )
    {
        return new StoreIndexDescriptor( this, id );
    }

    public StoreIndexDescriptor withIds( long id, long owningConstraintId )
    {
        assert owningConstraintId >= 0;
        return new StoreIndexDescriptor( this, id, owningConstraintId );
    }
}
