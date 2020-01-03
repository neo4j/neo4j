/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.kernel.api;

import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.values.storable.ValueCategory;

/**
 * Reference to a specific index together with it's capabilities. This reference is valid until the schema of the database changes
 * (that is a create/drop of an index or constraint occurs).
 */
public interface IndexReference extends IndexCapability
{
    String UNNAMED_INDEX = "Unnamed index";

    /**
     * Returns true if this index only allows one value per key.
     */
    boolean isUnique();

    /**
     * Returns the propertyKeyIds associated with this index.
     */
    int[] properties();

    /**
     * Returns the schema of this index.
     */
    SchemaDescriptor schema();

    /**
     * Returns the key (or name) of the index provider that backs this index.
     */
    String providerKey();

    /**
     * Returns the version of the index provider that backs this index.
     */
    String providerVersion();

    /**
     * The unique name for this index - either automatically generated or user supplied - or the {@link #UNNAMED_INDEX} constant.
     */
    String name();

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of what this index indexes.
     */
    String userDescription( TokenNameLookup tokenNameLookup );

    /**
     * Sorts indexes by type, returning first GENERAL indexes, followed by UNIQUE. Implementation is not suitable in
     * hot path.
     *
     * @param indexes Indexes to sort
     * @return sorted indexes
     */
    static Iterator<IndexReference> sortByType( Iterator<IndexReference> indexes )
    {
        List<IndexReference> materialized = Iterators.asList( indexes );
        return Iterators.concat(
                Iterators.filter( i -> !i.isUnique(), materialized.iterator() ),
                Iterators.filter( IndexReference::isUnique, materialized.iterator() ) );

    }

    IndexReference NO_INDEX = new IndexReference()
    {
        @Override
        public IndexOrder[] orderCapability( ValueCategory... valueCategories )
        {
            return NO_CAPABILITY.orderCapability( valueCategories );
        }

        @Override
        public IndexValueCapability valueCapability( ValueCategory... valueCategories )
        {
            return NO_CAPABILITY.valueCapability( valueCategories );
        }

        @Override
        public boolean isFulltextIndex()
        {
            return false;
        }

        @Override
        public boolean isEventuallyConsistent()
        {
            return false;
        }

        @Override
        public boolean isUnique()
        {
            return false;
        }

        @Override
        public int[] properties()
        {
            return new int[0];
        }

        @Override
        public SchemaDescriptor schema()
        {
            return SchemaDescriptor.NO_SCHEMA;
        }

        @Override
        public String providerKey()
        {
            return null;
        }

        @Override
        public String providerVersion()
        {
            return null;
        }

        @Override
        public String name()
        {
            return UNNAMED_INDEX;
        }

        @Override
        public String userDescription( TokenNameLookup tokenNameLookup )
        {
            return SchemaDescriptor.NO_SCHEMA.userDescription( tokenNameLookup );
        }
    };
}
