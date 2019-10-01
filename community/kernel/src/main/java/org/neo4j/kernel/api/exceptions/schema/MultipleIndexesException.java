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
package org.neo4j.kernel.api.exceptions.schema;

import java.util.Collection;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.String.format;
import static org.neo4j.common.TokenNameLookup.idTokenNameLookup;

public class MultipleIndexesException extends SchemaKernelException
{
    private final SchemaDescriptor descriptor;
    private final Collection<IndexDescriptor> indexes;
    private static final String message = "Multiple indexes were found for %s. Try using an index name instead: %s.";

    public MultipleIndexesException( SchemaDescriptor descriptor, Collection<IndexDescriptor> indexes )
    {
        super( Status.Schema.IndexMultipleFound, getUserMessage( idTokenNameLookup, descriptor, indexes ) );
        this.descriptor = descriptor;
        this.indexes = indexes;
    }

    @Override
    public String getUserMessage( TokenNameLookup tokenNameLookup )
    {
        return getUserMessage( tokenNameLookup, descriptor, indexes );
    }

    private static String getUserMessage( TokenNameLookup tokenNameLookup, SchemaDescriptor descriptor, Collection<IndexDescriptor> indexes )
    {
        return format( message, descriptor.userDescription( tokenNameLookup ), Iterators.toString( indexes.iterator(), IndexDescriptor::getName, 5 ) );
    }
}
