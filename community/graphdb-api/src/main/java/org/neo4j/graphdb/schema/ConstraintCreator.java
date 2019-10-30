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
package org.neo4j.graphdb.schema;

import java.util.Map;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.graphdb.ConstraintViolationException;

/**
 * A builder for entering details about a constraint to create. After all details have been entered
 * {@link #create()} must be called for the constraint to actually be created. A constraint creator knows
 * which {@link org.neo4j.graphdb.Label label} it is to be created for.
 *
 * All methods except {@link #create()} will return an {@link ConstraintCreator} which should be
 * used for further interaction.
 *
 * Compatibility note: New methods may be added to this interface without notice,
 * backwards compatibility is only guaranteed for clients of this interface, not for
 * implementors.
 *
 * @see Schema
 */
@PublicApi
public interface ConstraintCreator
{
    /**
     * Imposes a uniqueness constraint for the given property.
     * This means that there can be at most one node, having the given label, for any set value of that property key.
     *
     * @param propertyKey property to impose the uniqueness constraint for.
     * @return a {@link ConstraintCreator} instance to be used for further interaction.
     */
    ConstraintCreator assertPropertyIsUnique( String propertyKey );

    /**
     * Imposes an existence constraint for the given property.
     * This means that all nodes with the given label must have a value for this property.
     *
     * @param propertyKey property to impose the existence constraint for.
     * @return a {@link ConstraintCreator}  instance to be used for further interaction.
     */
    ConstraintCreator assertPropertyExists( String propertyKey );

    /**
     * Imposes both a uniqueness constraint, and a property existence constraint, for the given property.
     * This means that all nodes with the given label must have this property, and they must all have different values for the property.
     *
     * @param propertyKey property to use as the node key.
     * @return a {@link ConstraintCreator} instance to be used for further interaction.
     */
    ConstraintCreator assertPropertyIsNodeKey( String propertyKey );

    /**
     * Assign a name to the constraint, which will then be returned from {@link ConstraintDefinition#getName()}, and can be used for finding the constraint
     * with {@link Schema#getConstraintByName(String)}, or the associated index with {@link Schema#getIndexByName(String)} for index backed constraints.
     *
     * @param name the name to give the constraint.
     * @return a {@link ConstraintCreator} instance to be used for further interaction.
     */
    ConstraintCreator withName( String name );

    /**
     * Assign an index type to the constraint. If the constraint is not backed by an index, then the presence of an index type will cause {@link #create()} to
     * throw an exception.
     *
     * @param indexType the type of index wanted for backing the constraint.
     * @return a {@link ConstraintCreator} instance to be used for further interaction.
     * @see IndexType
     */
    ConstraintCreator withIndexType( IndexType indexType );

    /**
     * Set index-specific index configurations. If the constraint is not backed by an index, then the presence of an index configuration will cause
     * {@link #create()} to throw an exception.
     * <p>
     * This call will override the settings from any previous call to this method.
     *
     * @param indexConfiguration The index settings in the index configuration that differ from their defaults.
     * @return a {@link ConstraintCreator} instance to be used for further interaction.
     */
    ConstraintCreator withIndexConfiguration( Map<IndexSetting,Object> indexConfiguration );

    /**
     * Creates a constraint with the details specified by the other methods in this interface.
     *
     * @return the created {@link ConstraintDefinition constraint}.
     * @throws ConstraintViolationException if creating this constraint would violate any
     * existing constraints.
     */
    ConstraintDefinition create() throws ConstraintViolationException;
}
