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
package org.neo4j.graphdb.schema;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

/**
 * A builder for entering details about an index to create. After all details have been entered
 * {@link #create()} must be called for the index to actually be created. An index creator knows
 * which {@link Label label} it is to be created for.
 * 
 * All methods except {@link #create()} will return an {@link IndexCreator} which should be
 * used for further interaction.
 * 
 * @see Schema
 */
public interface IndexCreator
{
    /**
     * Includes the given {@code propertyKey} in this index, such that {@link Node nodes} with
     * the assigned {@link Label label} and this property key will have its values indexed.
     * 
     * NOTE: currently only a single property key per index is supported.
     * 
     * @param propertyKey the property key to include in this index to be created.
     * @return an {@link IndexCreator} instance to be used for further interaction.
     */
    IndexCreator on( String propertyKey );
    
    /**
     * Creates an index with the details specified by the other methods in this interface.
     * 
     * @return the created {@link IndexDefinition index}.
     * @throws ConstraintViolationException if creating this index would violate one or more constraints.
     */
    IndexDefinition create() throws ConstraintViolationException;
}
