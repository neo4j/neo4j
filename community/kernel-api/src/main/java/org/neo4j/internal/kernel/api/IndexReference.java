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
package org.neo4j.internal.kernel.api;

import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;

/**
 * Reference to a specific index. This reference is valid until the schema of the database changes (that is a
 * create/drop of an index or constraint occurs).
 */
public interface IndexReference
{
    boolean isUnique();

    int label();

    int[] properties();

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of what this index indexes.
     */
    default String userDescription( TokenNameLookup tokenNameLookup )
    {
        String type = isUnique() ? "UNIQUE" : "GENERAL";
        String schemaDescription = SchemaUtil.niceLabelAndProperties( tokenNameLookup, label(), properties() );
        return SchemaUtil.withType( type, schemaDescription );
    }

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
}
