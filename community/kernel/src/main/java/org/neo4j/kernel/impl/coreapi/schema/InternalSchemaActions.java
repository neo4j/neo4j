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
package org.neo4j.kernel.impl.coreapi.schema;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;

/**
 * Implementations are used to configure {@link IndexCreatorImpl} and {@link BaseNodeConstraintCreator} for re-use
 * by both the graph database and the batch inserter.
 */
public interface InternalSchemaActions
{
    IndexDefinition createIndexDefinition( Label label, String propertyKey );

    void dropIndexDefinitions( Label label, String propertyKey );

    ConstraintDefinition createPropertyUniquenessConstraint( Label label, String propertyKey )
            throws IllegalTokenNameException, TooManyLabelsException, CreateConstraintFailureException,
                   AlreadyConstrainedException, AlreadyIndexedException;

    ConstraintDefinition createPropertyExistenceConstraint( Label label, String propertyKey )
            throws IllegalTokenNameException, TooManyLabelsException, CreateConstraintFailureException,
            AlreadyConstrainedException;

    ConstraintDefinition createPropertyExistenceConstraint( RelationshipType type, String propertyKey )
            throws CreateConstraintFailureException, AlreadyConstrainedException;

    void dropPropertyUniquenessConstraint( Label label, String propertyKey );

    void dropNodePropertyExistenceConstraint( Label label, String propertyKey );

    void dropRelationshipPropertyExistenceConstraint( RelationshipType type, String propertyKey );

    String getUserMessage( KernelException e );

    void assertInUnterminatedTransaction();
}
