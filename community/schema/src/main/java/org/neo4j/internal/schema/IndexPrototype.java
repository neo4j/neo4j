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

import java.util.Objects;
import java.util.Optional;

import org.neo4j.common.TokenNameLookup;

import static org.neo4j.internal.schema.IndexType.BTREE;

/**
 * The prototype of an index that may or may not exist.
 */
public class IndexPrototype implements IndexRef<IndexPrototype>
{
    private final SchemaDescriptor schema;
    private final boolean isUnique;
    private final IndexProviderDescriptor indexProvider;
    private final String name;
    private final IndexType indexType;
    private final IndexConfig indexConfig;

    public static IndexPrototype forSchema( SchemaDescriptor schema )
    {
        return new IndexPrototype( schema, false, IndexProviderDescriptor.UNDECIDED, null, BTREE, IndexConfig.empty() );
    }

    public static IndexPrototype forSchema( SchemaDescriptor schema, IndexProviderDescriptor indexProvider )
    {
        return new IndexPrototype( schema, false, indexProvider, null, BTREE, IndexConfig.empty() );
    }

    public static IndexPrototype uniqueForSchema( SchemaDescriptor schema )
    {
        return new IndexPrototype( schema, true, IndexProviderDescriptor.UNDECIDED, null, BTREE, IndexConfig.empty() );
    }

    public static IndexPrototype uniqueForSchema( SchemaDescriptor schema, IndexProviderDescriptor indexProvider )
    {
        return new IndexPrototype( schema, true, indexProvider, null, BTREE, IndexConfig.empty() );
    }

    private IndexPrototype( SchemaDescriptor schema, boolean isUnique, IndexProviderDescriptor indexProvider, String name, IndexType indexType,
            IndexConfig indexConfig )
    {
        Objects.requireNonNull( schema, "Schema of index cannot be null." );
        Objects.requireNonNull( indexProvider, "Index provider cannot be null." );
        Objects.requireNonNull( indexType, "Index type cannot be null." );
        Objects.requireNonNull( indexConfig, "Index configuration cannot be null." );
        // Note that 'name' is allowed to be null in the constructor, as that is the case initially for new index prototypes.

        this.schema = schema;
        this.isUnique = isUnique;
        this.indexProvider = indexProvider;
        this.name = name;
        this.indexType = indexType;
        this.indexConfig = indexConfig;
    }

    @Override
    public SchemaDescriptor schema()
    {
        return schema;
    }

    @Override
    public boolean isUnique()
    {
        return isUnique;
    }

    /**
     * Get the name of this index prototype, if any.
     * @return An optional representing the name of this index prototype, if any.
     */
    public Optional<String> getName()
    {
        return Optional.ofNullable( name );
    }

    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return "Index( " + (name == null ? "" : name + ", ") + (isUnique ? "UNIQUE" : "GENERAL") + ", " +
                schema().userDescription( tokenNameLookup )  + ", " +
                getIndexProvider().name() + " )";
    }

    @Override
    public String toString()
    {
        return userDescription( TokenNameLookup.idTokenNameLookup );
    }

    @Override
    public IndexType getIndexType()
    {
        return indexType;
    }

    @Override
    public IndexProviderDescriptor getIndexProvider()
    {
        return indexProvider;
    }

    @Override
    public IndexPrototype withIndexProvider( IndexProviderDescriptor indexProvider )
    {
        return new IndexPrototype( schema, isUnique, indexProvider, name, indexType, indexConfig );
    }

    @Override
    public IndexPrototype withSchemaDescriptor( SchemaDescriptor schema )
    {
        return new IndexPrototype( schema, isUnique, indexProvider, name, indexType, indexConfig );
    }

    @Override
    public IndexConfig getIndexConfig()
    {
        return indexConfig;
    }

    @Override
    public IndexPrototype withIndexConfig( IndexConfig indexConfig )
    {
        if ( indexConfig == null )
        {
            return this;
        }
        return new IndexPrototype( schema, isUnique, indexProvider, name, indexType, indexConfig );
    }

    /**
     * Produce a new index prototype that is the same as this index prototype in every way, except it has the given name.
     * If the given name is null, then this index descriptor is returned unchanged.
     *
     * @param name The name used in the new index prototype.
     * @return A new index prototype with the given name.
     */
    public IndexPrototype withName( String name )
    {
        if ( name == null )
        {
            return this;
        }
        return new IndexPrototype( schema, isUnique, indexProvider, name, indexType, indexConfig );
    }

    /**
     * Produce a new index prototype that is the same as this index prototype in every way, except it has the given index type.
     *
     * @param indexType The index type assigned to the new index prototype.
     * @return A new index prototype with the given index type.
     */
    public IndexPrototype withIndexType( IndexType indexType )
    {
        if ( indexType == null )
        {
            return this;
        }
        return new IndexPrototype( schema, isUnique, indexProvider, name, indexType, indexConfig );
    }

    /**
     * Materialise this index prototype into a real index descriptor with the given index id.
     *
     * @param id The real, final, id of the index.
     * @return An index descriptor identifying the physical index derived from this index prototype.
     */
    public IndexDescriptor materialise( long id )
    {
        return new IndexDescriptor( id, this );
    }
}
