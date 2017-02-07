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
package org.neo4j.kernel.api.schema_new;

import org.neo4j.kernel.api.TokenNameLookup;

/**
 * Internal representation of one schema unit, for example a label-property pair.
 *
 * Even when only supporting a small set of different schemas, the number of common methods is very small. This
 * interface therefore supports a visitor type access pattern, results can be computed using the {#compute} method, and
 * side-effect type logic performed using the processWith method. This means that when implementing this interface
 * with a new concrete type, the compute and processWith method implementations need to be added similarly to
 * how this is done in eg. LabelSchemaDescriptor, and the SchemaProcessor and SchemaComputer interfaces need to be
 * extended with methods taking the new concrete type as argument.
 */
public interface SchemaDescriptor
{
    /**
     * Computes some value by feeding this object into the given SchemaComputer.
     *
     * Note that implementers of this method just need to call `return computer.compute( this );`.
     *
     * @param computer The SchemaComputer that hold the logic for the computation
     * @param <R> The return type
     * @return The result of the computation
     */
    <R> R computeWith( SchemaComputer<R> computer );

    /**
     * Performs some side-effect type logic by processing this object using the given SchemaProcessor.
     *
     * Note that implementers of this method just need to call `return processor.process( this );`.
     *
     * @param processor The SchemaProcessor that hold the logic for the computation
     */
    void processWith( SchemaProcessor processor );

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of what this index indexes.
     */
    String userDescription( TokenNameLookup tokenNameLookup );

    /**
     * Checks whether a schema descriptor Supplier supplies this schema descriptor.
     * @param supplier supplier to get a schema descriptor from
     * @return true if the supplied schema descriptor equals this schema descriptor
     */
    default boolean isSame( SchemaDescriptor.Supplier supplier )
    {
        return this.equals( supplier.schema() );
    }

    interface Supplier
    {
        SchemaDescriptor schema();
    }
}
