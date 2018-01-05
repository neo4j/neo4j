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
package org.neo4j.internal.kernel.api;

import java.util.Iterator;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;

/**
 * Surface for getting schema information, such as fetching specific indexes or constraints.
 */
public interface SchemaRead
{
    /**
     * Acquire a reference to the index mapping the given {@code label} and {@code properties}.
     *
     * @param label the index label
     * @param properties the index properties
     * @return the IndexReference, or {@link CapableIndexReference#NO_INDEX} if such an index does not exist.
     */
    CapableIndexReference index( int label, int... properties );

    /**
     * Finds all constraints for the given schema
     * @param descriptor The descriptor of the schema
     * @return All constraints for the given schema
     */
    Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor );

    /**
     * Checks if a constraint exists
     * @param descriptor The descriptor of the constraint to check.
     * @return <tt>true</tt> if the constraint exists, otherwise <tt>false</tt>
     */
    boolean constraintExists( ConstraintDescriptor descriptor );

    /**
     * Finds all constraints for the given label
     * @param labelId The id of the label
     * @return All constraints for the given label
     */
    Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId );

    /**
     * Find all constraints in the database
     * @return An iterator of all the constraints in the database.
     */
    Iterator<ConstraintDescriptor> constraintsGetAll( );
}
