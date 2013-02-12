/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import java.util.Collection;

import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;

/**
 * TODO temporary name
 *
 * Used when committing for notifying schema indexes about updates made to the graph.
 * Currently in the form of {@link PropertyRecord property records}.
 */
public class SchemaIndexing
{
    // To use PropertyRecord here might not be very good.
    void apply( Collection<PropertyRecord> propertyRecords )
    {
        // TODO look at the schema and feed the properties to the appropriate indexes.
        //      This gets called when committing a transaction.
        throw new UnsupportedOperationException();
    }
}
