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
package org.neo4j.internal.kernel.api.security;

import java.net.InetAddress;
import java.net.URI;
import java.util.function.Supplier;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.neo4j.internal.kernel.api.RelTypeSupplier;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.storageengine.api.PropertySelection;

public class TestAccessMode implements AccessMode {
    private final boolean allowRead;
    private final boolean allowReadAll;
    private final boolean allowWrite;
    private final boolean allowSchema;

    private final boolean allowLoad;

    public TestAccessMode(
            boolean allowRead, boolean allowReadAll, boolean allowWrite, boolean allowSchema, boolean allowLoad) {
        this.allowRead = allowRead;
        this.allowReadAll = allowReadAll;
        this.allowWrite = allowWrite;
        this.allowSchema = allowSchema;
        this.allowLoad = allowLoad;
    }

    @Override
    public boolean allowsWrites() {
        return allowWrite;
    }

    @Override
    public PermissionState allowsTokenCreates(PrivilegeAction action) {
        return PermissionState.fromAllowList(allowWrite);
    }

    @Override
    public boolean allowsSchemaWrites() {
        return allowSchema;
    }

    @Override
    public PermissionState allowsSchemaWrites(PrivilegeAction action) {
        return PermissionState.fromAllowList(allowSchema);
    }

    @Override
    public boolean allowsShowIndex() {
        return allowSchema;
    }

    @Override
    public boolean allowsShowConstraint() {
        return allowSchema;
    }

    @Override
    public boolean allowsTraverseAllLabels() {
        return allowReadAll;
    }

    @Override
    public boolean allowsTraverseAllNodesWithLabel(int label) {
        return allowReadAll;
    }

    @Override
    public boolean disallowsTraverseLabel(int label) {
        return !allowRead;
    }

    @Override
    public boolean allowsTraverseNode(int... labels) {
        return allowRead;
    }

    @Override
    public IntSet getTraverseSecurityProperties(int[] labels) {
        return IntSets.immutable.empty();
    }

    @Override
    public boolean hasApplicableTraverseAllowPropertyRules(int label) {
        return allowRead;
    }

    @Override
    public boolean allowsTraverseNodeWithPropertyRules(ReadSecurityPropertyProvider propertyProvider, int... labels) {
        return allowRead;
    }

    @Override
    public boolean hasTraversePropertyRules() {
        return false;
    }

    @Override
    public boolean allowsTraverseAllRelTypes() {
        return allowReadAll;
    }

    @Override
    public boolean allowsTraverseRelType(int relType) {
        return allowRead;
    }

    @Override
    public boolean disallowsTraverseRelType(int relType) {
        return !allowRead;
    }

    @Override
    public boolean allowsReadPropertyAllLabels(int propertyKey) {
        return allowReadAll;
    }

    @Override
    public boolean disallowsReadPropertyForSomeLabel(int propertyKey) {
        return !allowReadAll;
    }

    @Override
    public boolean allowsReadNodeProperties(
            Supplier<TokenSet> labels, int[] propertyKeys, ReadSecurityPropertyProvider propertyProvider) {
        return allowRead;
    }

    @Override
    public boolean allowsReadNodeProperties(Supplier<TokenSet> labels, int[] propertyKeys) {
        return allowRead;
    }

    @Override
    public boolean allowsReadNodeProperty(
            Supplier<TokenSet> labels, int propertyKey, ReadSecurityPropertyProvider propertyProvider) {
        return allowRead;
    }

    @Override
    public boolean allowsReadNodeProperty(Supplier<TokenSet> labels, int propertyKey) {
        return allowRead;
    }

    @Override
    public boolean allowsReadPropertyAllRelTypes(int propertyKey) {
        return allowReadAll;
    }

    @Override
    public boolean allowsReadRelationshipProperty(RelTypeSupplier relType, int propertyKey) {
        return allowRead;
    }

    @Override
    public boolean allowsSeePropertyKeyToken(int propertyKey) {
        return allowRead;
    }

    @Override
    public boolean hasPropertyReadRules() {
        return false;
    }

    @Override
    public boolean hasPropertyReadRules(int... propertyKeys) {
        return false;
    }

    @Override
    public IntSet getReadSecurityProperties(int propertyKey) {
        return IntSets.immutable.empty();
    }

    @Override
    public IntSet getAllReadSecurityProperties() {
        return IntSets.immutable.empty();
    }

    @Override
    public PropertySelection getSecurityPropertySelection(PropertySelection selection) {
        return PropertySelection.NO_PROPERTIES;
    }

    @Override
    public PermissionState allowsExecuteProcedure(int procedureId) {
        return PermissionState.EXPLICIT_GRANT;
    }

    @Override
    public PermissionState shouldBoostProcedure(int procedureId) {
        return PermissionState.EXPLICIT_GRANT;
    }

    @Override
    public PermissionState allowsExecuteFunction(int id) {
        return PermissionState.EXPLICIT_GRANT;
    }

    @Override
    public PermissionState shouldBoostFunction(int id) {
        return PermissionState.EXPLICIT_GRANT;
    }

    @Override
    public PermissionState allowsExecuteAggregatingFunction(int id) {
        return PermissionState.EXPLICIT_GRANT;
    }

    @Override
    public PermissionState shouldBoostAggregatingFunction(int id) {
        return PermissionState.EXPLICIT_GRANT;
    }

    @Override
    public PermissionState allowsShowSetting(String setting) {
        return PermissionState.EXPLICIT_GRANT;
    }

    @Override
    public boolean allowsSetLabel(int labelId) {
        return allowWrite;
    }

    @Override
    public boolean allowsRemoveLabel(int labelId) {
        return allowWrite;
    }

    @Override
    public boolean allowsCreateNode(int[] labelIds) {
        return allowWrite;
    }

    @Override
    public boolean allowsDeleteNode(Supplier<TokenSet> labelSupplier) {
        return allowWrite;
    }

    @Override
    public boolean allowsCreateRelationship(int relType) {
        return allowWrite;
    }

    @Override
    public boolean allowsDeleteRelationship(int relType) {
        return allowWrite;
    }

    @Override
    public boolean allowsSetProperty(Supplier<TokenSet> labels, int propertyKey) {
        return allowWrite;
    }

    @Override
    public boolean allowsSetProperty(RelTypeSupplier relType, int propertyKey) {
        return allowWrite;
    }

    @Override
    public PermissionState allowsLoadAllData() {
        return PermissionState.fromAllowList(allowLoad);
    }

    @Override
    public PermissionState allowsLoadUri(URI url, InetAddress inetAddress) {
        return PermissionState.fromAllowList(allowLoad);
    }

    @Override
    public String name() {
        return "Test";
    }
}
