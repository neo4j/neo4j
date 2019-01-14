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
package org.neo4j.internal.kernel.api.schema;

/**
 * A SchemaComputer computes values of type R from SchemaDescriptors. To get the concrete type of the target schema
 * descriptor, a visitor pattern is used to bounce the code path into the correct overloaded computeSpecific variant.
 * Perhaps the most useful way to view this, is to think of the computer as a switch on the argument .getClass, and
 * the computeSpecific as correctly typed cases. The benefit of using the visitor pattern, is that implementers of
 * new concrete SchemaDescriptor implementers will be forced to implement the necessary methods at compile time,
 * avoiding bugs due to missed instance of checks.
 *
 * DANGER: Visitor patterns can be very hard to follow and debug. Take care to make callers easily readable!
 *
 * @param <R> the computation result type
 */
public interface SchemaComputer<R>
{
    /*
    The following section contains the overloaded process signatures for all concrete SchemaDescriptor implementers.
    Add new overloaded methods here when adding more concrete SchemaDescriptors.
     */
    R computeSpecific( LabelSchemaDescriptor schema );
    R computeSpecific( RelationTypeSchemaDescriptor schema );
}
