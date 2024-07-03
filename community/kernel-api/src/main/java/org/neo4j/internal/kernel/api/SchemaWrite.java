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
package org.neo4j.internal.kernel.api;

import java.util.List;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.EndpointType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.RelationshipEndpointSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;

/**
 * Surface for creating and dropping indexes and constraints.
 */
public interface SchemaWrite {
    /**
     * Translate an index provider name, into an {@link IndexProviderDescriptor}.
     *
     * This method is only used when creating indexes using custom or specific index providers.
     * Normally, {@link IndexType} would be used instead, but this is not always accurate enough.
     *
     * @param providerName The name of the index provider to resolve the descriptor for.
     * @return The provider descriptor with the given provider name.
     * @throws RuntimeException if there is no index provider by the given name.
     */
    IndexProviderDescriptor indexProviderByName(String providerName);

    /**
     * Translate an index provider name, into an {@link IndexType}.
     *
     * This method is only used when creating constraints using custom or specific index providers.
     *
     * @param providerName The name of the index provider to resolve the type for.
     * @return The index type that the given provider corresponds to.
     * @throws RuntimeException if there is no index provider by the given name.
     */
    IndexType indexTypeByProviderName(String providerName);

    /**
     * Get all the index providers that has support for the given {@link IndexType}.
     *
     * @param indexType {@link IndexType} to find index providers for
     * @return List of all {@link IndexProviderDescriptor} that describe index provider with support for the given
     * index type.
     */
    List<IndexProviderDescriptor> indexProvidersByType(IndexType indexType);

    /**
     * Create index using the given {@link IndexPrototype}.
     *
     * @param prototype the prototype specifying the relevant schema and configuration of the index to create.
     * @return the {@link IndexDescriptor} for the created index.
     * @throws KernelException if the index cannot be created for some reason.
     */
    IndexDescriptor indexCreate(IndexPrototype prototype) throws KernelException;

    /**
     * Drop the given index
     *
     * @param index the index to drop
     */
    void indexDrop(IndexDescriptor index) throws SchemaKernelException;

    /**
     * Drop the index by the given name.
     * @param indexName the name of the index to drop.
     */
    void indexDrop(String indexName) throws SchemaKernelException;

    /**
     * Create a unique property constraint based on the given uniqueness index prototype.
     * The given index prototype will be used for creating the uniqueness index backing the constraint.
     *
     * @param prototype a prototype that describes the constraint index, and includes the schema of the constraint.
     * @return The {@link ConstraintDescriptor} of the created constraint.
     * @throws KernelException if the constraint cannot be created for some reason.
     */
    ConstraintDescriptor uniquePropertyConstraintCreate(IndexPrototype prototype) throws KernelException;

    /**
     * Create node/relationship key constraint based on the given uniqueness index prototype.
     * The given index prototype will be used for creating the uniqueness index backing the constraint.
     *
     * @param prototype the index prototype for which to create a node/relationship key constraint.
     * @return the created constraint.
     * @throws KernelException if the constraint cannot be created for some reason.
     */
    ConstraintDescriptor keyConstraintCreate(IndexPrototype prototype) throws KernelException;

    /**
     * Create node property existence constraint
     *
     * @param schema descriptor of the constraint
     * @param isDependent graph type dependence
     */
    ConstraintDescriptor nodePropertyExistenceConstraintCreate(
            LabelSchemaDescriptor schema, String name, boolean isDependent) throws KernelException;

    /**
     * Create relationship property existence constraint
     *
     * @param schema descriptor of the constraint
     * @param isDependent graph type dependence
     */
    ConstraintDescriptor relationshipPropertyExistenceConstraintCreate(
            RelationTypeSchemaDescriptor schema, String name, boolean isDependent) throws KernelException;

    /**
     * Create property type constraint
     *
     * @param schema descriptor of the constraint
     * @param name the name the created constraint should have, or null
     * @param propertyType the allowed property types
     * @param isDependent  graph type dependence
     * @return the created constraint.
     * @throws KernelException if the constraint cannot be created for some reason.
     */
    ConstraintDescriptor propertyTypeConstraintCreate(
            SchemaDescriptor schema, String name, PropertyTypeSet propertyType, boolean isDependent)
            throws KernelException;

    /**
     * Create a relationship endpoint constraint
     *
     * @param name the name the created constraint should have, or null (when null a generated name will be chosen)
     * @param schema descriptor of entities that the constraint applies to
     * @param endpointLabelId the token id for the Label that the endpoint will be required to have
     * @param endpointType the {@link EndpointType} of the node which we are constraining on
     * @return the created constraint
     * @throws KernelException
     */
    ConstraintDescriptor relationshipEndpointConstraintCreate(
            RelationshipEndpointSchemaDescriptor schema, String name, int endpointLabelId, EndpointType endpointType)
            throws KernelException;

    /**
     * Drop the constraint with the given name.
     *
     * @param name             The name of the constraint to be dropped.
     * @param canDropDependent if the drop is privileged to drop a dependent constraint
     */
    void constraintDrop(String name, boolean canDropDependent) throws SchemaKernelException;

    /**
     * Drop the specific constraint.
     *
     * @param constraint descriptor of the constraint
     * @param canDropDependent if the drop is privileged to drop a dependent constraint
     */
    void constraintDrop(ConstraintDescriptor constraint, boolean canDropDependent) throws SchemaKernelException;
}
