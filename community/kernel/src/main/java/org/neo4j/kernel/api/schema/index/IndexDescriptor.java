/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.function.Predicate;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.LabelSchemaSupplier;
import org.neo4j.kernel.api.schema.SchemaUtil;

import static java.lang.String.format;
import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Filter.GENERAL;
import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Filter.UNIQUE;

/**
 * Internal representation of a graph index, including the schema unit it targets (eg. label-property combination)
 * and the type of index. UNIQUE indexes are used to back uniqueness constraints.
 *
 * This will be renamed to IndexDescriptor, once the old org.neo4j.kernel.api.schema.IndexDescriptor is completely
 * removed.
 */
public class IndexDescriptor implements LabelSchemaSupplier
{
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

    private final LabelSchemaDescriptor schema;
    private final IndexDescriptor.Type type;

    IndexDescriptor( LabelSchemaDescriptor schema, Type type )
    {
        this.schema = schema;
        this.type = type;
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
    public LabelSchemaDescriptor schema()
    {
        return schema;
    }

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of what this index indexes.
     */
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return format( "Index( %s, %s )", type.name(), schema.userDescription( tokenNameLookup ) );
    }

    /**
     * Checks whether an index descriptor Supplier supplies this index descriptor.
     * @param supplier supplier to get a index descriptor from
     * @return true if the supplied index descriptor equals this index descriptor
     */
    public boolean isSame( Supplier supplier )
    {
        return this.equals( supplier.getIndexDescriptor() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o != null && o instanceof IndexDescriptor )
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

    /**
     * Sorts indexes by type, returning first GENERAL indexes, followed by UNIQUE. Implementation is not suitable in
     * hot path.
     *
     * @param indexes Indexes to sort
     * @return sorted indexes
     */
    public static Iterator<IndexDescriptor> sortByType( Iterator<IndexDescriptor> indexes )
    {
        List<IndexDescriptor> materialized = Iterators.asList( indexes );
        return Iterators.concat(
                Iterators.filter( GENERAL, materialized.iterator() ),
                Iterators.filter( UNIQUE, materialized.iterator() ) );

    }
}
