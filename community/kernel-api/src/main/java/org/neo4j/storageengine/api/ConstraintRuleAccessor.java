/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api;

import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.constraints.KeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.LabelCoexistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelationshipEndpointConstraintDescriptor;
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;

public interface ConstraintRuleAccessor {
    ConstraintDescriptor readConstraint(ConstraintDescriptor rule);

    ConstraintDescriptor createUniquenessConstraintRule(
            long ruleId, UniquenessConstraintDescriptor descriptor, long indexId);

    ConstraintDescriptor createKeyConstraintRule(long ruleId, KeyConstraintDescriptor descriptor, long indexId)
            throws CreateConstraintFailureException;

    ConstraintDescriptor createExistenceConstraint(long ruleId, ConstraintDescriptor descriptor)
            throws CreateConstraintFailureException;

    ConstraintDescriptor createPropertyTypeConstraint(long ruleId, TypeConstraintDescriptor descriptor)
            throws CreateConstraintFailureException;

    ConstraintDescriptor createRelationshipEndpointConstraint(
            long ruleId, RelationshipEndpointConstraintDescriptor descriptor) throws CreateConstraintFailureException;

    ConstraintDescriptor createLabelCoexistenceConstraint(long ruleId, LabelCoexistenceConstraintDescriptor descriptor)
            throws CreateConstraintFailureException;
}
