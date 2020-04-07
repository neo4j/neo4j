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
package org.neo4j.kernel.impl.store;

import java.util.Collections;
import java.util.Set;

import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.schema.IndexDescriptor;

import static java.lang.String.format;

public class MultipleUnderlyingStorageExceptions extends UnderlyingStorageException
{
    public final Set<Pair<IndexDescriptor, UnderlyingStorageException>> exceptions;

    public MultipleUnderlyingStorageExceptions( Set<Pair<IndexDescriptor, UnderlyingStorageException>> exceptions )
    {
        super( buildMessage( exceptions ) );
        this.exceptions = Collections.unmodifiableSet( exceptions );

        for ( Pair<IndexDescriptor, UnderlyingStorageException> exception : exceptions )
        {
            this.addSuppressed( exception.other() );
        }
    }

    private static String buildMessage( Set<Pair<IndexDescriptor, UnderlyingStorageException>> exceptions )
    {
        StringBuilder builder = new StringBuilder( );
        builder.append("Errors when closing (flushing) index updaters:");

        for ( Pair<IndexDescriptor, UnderlyingStorageException> pair : exceptions )
        {
            builder.append( format( " (%s) %s", pair.first(), pair.other().getMessage() ) );
        }

        return builder.toString();
    }
}
