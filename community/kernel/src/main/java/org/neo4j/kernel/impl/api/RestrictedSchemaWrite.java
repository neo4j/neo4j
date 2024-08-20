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
package org.neo4j.kernel.impl.api;

import java.util.List;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.EndpointType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.LabelCoexistenceSchemaDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.RelationshipEndpointSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;

public class RestrictedSchemaWrite implements SchemaWrite {
    private final SchemaWrite inner;
    private final SecurityContext securityContext;
    private final SecurityAuthorizationHandler securityAuthorizationHandler;

    RestrictedSchemaWrite(
            SchemaWrite inner,
            SecurityContext securityContext,
            SecurityAuthorizationHandler securityAuthorizationHandler) {
        this.inner = inner;
        this.securityContext = securityContext;
        this.securityAuthorizationHandler = securityAuthorizationHandler;
    }

    @Override
    public IndexProviderDescriptor indexProviderByName(String providerName) {
        return inner.indexProviderByName(providerName);
    }

    @Override
    public IndexType indexTypeByProviderName(String providerName) {
        return inner.indexTypeByProviderName(providerName);
    }

    @Override
    public List<IndexProviderDescriptor> indexProvidersByType(IndexType indexType) {
        return inner.indexProvidersByType(indexType);
    }

    @Override
    public IndexDescriptor indexCreate(IndexPrototype prototype) throws KernelException {
        securityAuthorizationHandler.assertSchemaWrites(securityContext, PrivilegeAction.CREATE_INDEX);
        return inner.indexCreate(prototype);
    }

    @Override
    public void indexDrop(IndexDescriptor index) throws SchemaKernelException {
        securityAuthorizationHandler.assertSchemaWrites(securityContext, PrivilegeAction.DROP_INDEX);
        inner.indexDrop(index);
    }

    @Override
    public void indexDrop(String indexName) throws SchemaKernelException {
        securityAuthorizationHandler.assertSchemaWrites(securityContext, PrivilegeAction.DROP_INDEX);
        inner.indexDrop(indexName);
    }

    @Override
    public ConstraintDescriptor uniquePropertyConstraintCreate(IndexPrototype prototype) throws KernelException {
        securityAuthorizationHandler.assertSchemaWrites(securityContext, PrivilegeAction.CREATE_CONSTRAINT);
        return inner.uniquePropertyConstraintCreate(prototype);
    }

    @Override
    public ConstraintDescriptor keyConstraintCreate(IndexPrototype prototype) throws KernelException {
        securityAuthorizationHandler.assertSchemaWrites(securityContext, PrivilegeAction.CREATE_CONSTRAINT);
        return inner.keyConstraintCreate(prototype);
    }

    @Override
    public ConstraintDescriptor nodePropertyExistenceConstraintCreate(
            LabelSchemaDescriptor schema, String name, boolean isDependent) throws KernelException {
        securityAuthorizationHandler.assertSchemaWrites(securityContext, PrivilegeAction.CREATE_CONSTRAINT);
        return inner.nodePropertyExistenceConstraintCreate(schema, name, isDependent);
    }

    @Override
    public ConstraintDescriptor relationshipPropertyExistenceConstraintCreate(
            RelationTypeSchemaDescriptor schema, String name, boolean isDependent) throws KernelException {
        securityAuthorizationHandler.assertSchemaWrites(securityContext, PrivilegeAction.CREATE_CONSTRAINT);
        return inner.relationshipPropertyExistenceConstraintCreate(schema, name, isDependent);
    }

    @Override
    public ConstraintDescriptor propertyTypeConstraintCreate(
            SchemaDescriptor schema, String name, PropertyTypeSet propertyType, boolean isDependent)
            throws KernelException {
        securityAuthorizationHandler.assertSchemaWrites(securityContext, PrivilegeAction.CREATE_CONSTRAINT);
        return inner.propertyTypeConstraintCreate(schema, name, propertyType, isDependent);
    }

    @Override
    public ConstraintDescriptor relationshipEndpointConstraintCreate(
            RelationshipEndpointSchemaDescriptor schema, String name, int endpointLabelId, EndpointType endpointType)
            throws KernelException {
        securityAuthorizationHandler.assertSchemaWrites(securityContext, PrivilegeAction.CREATE_CONSTRAINT);
        return inner.relationshipEndpointConstraintCreate(schema, name, endpointLabelId, endpointType);
    }

    @Override
    public ConstraintDescriptor labelCoexistenceConstraintCreate(
            LabelCoexistenceSchemaDescriptor schema, String name, int requiredLabelId) throws KernelException {
        securityAuthorizationHandler.assertSchemaWrites(securityContext, PrivilegeAction.CREATE_CONSTRAINT);
        return inner.labelCoexistenceConstraintCreate(schema, name, requiredLabelId);
    }

    @Override
    public void constraintDrop(String name, boolean canDropDependent) throws SchemaKernelException {
        securityAuthorizationHandler.assertSchemaWrites(securityContext, PrivilegeAction.DROP_CONSTRAINT);
        inner.constraintDrop(name, canDropDependent);
    }

    @Override
    public void constraintDrop(ConstraintDescriptor constraint, boolean canDropDependent) throws SchemaKernelException {
        securityAuthorizationHandler.assertSchemaWrites(securityContext, PrivilegeAction.DROP_CONSTRAINT);
        inner.constraintDrop(constraint, canDropDependent);
    }
}
