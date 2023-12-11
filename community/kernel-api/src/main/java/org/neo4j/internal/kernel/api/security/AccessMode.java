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
import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.neo4j.internal.kernel.api.RelTypeSupplier;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.storageengine.api.PropertySelection;

/**
 * Controls the capabilities of a KernelTransaction.
 */
public interface AccessMode {

    enum Static implements AccessMode {
        /**
         * No reading or writing allowed.
         */
        ACCESS(false, false, false, false, false),
        /**
         * No reading or writing allowed because of expired credentials.
         */
        CREDENTIALS_EXPIRED(false, false, false, false, false),

        /**
         * Allows reading data and schema, but not writing.
         */
        READ(true, false, false, false, false),
        /**
         * Allows writing data
         */
        WRITE_ONLY(false, true, false, false, false),
        /**
         * Allows reading and writing data, but not schema.
         */
        WRITE(true, true, false, false, false),
        /**
         * Allows reading and writing data and creating new tokens, but not schema.
         */
        TOKEN_WRITE(true, true, true, false, false),
        /**
         * Allows reading and writing data and creating new tokens and changing schema.
         */
        SCHEMA(true, true, true, true, false),
        /**
         * Allows all operations.
         */
        FULL(true, true, true, true, true);

        private final boolean read;
        private final boolean write;
        private final boolean token;
        private final boolean schema;
        private final boolean procedureBoost;

        Static(boolean read, boolean write, boolean token, boolean schema, boolean procedureBoost) {
            this.read = read;
            this.write = write;
            this.token = token;
            this.schema = schema;
            this.procedureBoost = procedureBoost;
        }

        @Override
        public boolean allowsWrites() {
            return write;
        }

        @Override
        public PermissionState allowsTokenCreates(PrivilegeAction action) {
            return PermissionState.fromAllowList(token);
        }

        @Override
        public boolean allowsSchemaWrites() {
            return schema;
        }

        @Override
        public PermissionState allowsSchemaWrites(PrivilegeAction action) {
            return PermissionState.fromAllowList(schema);
        }

        @Override
        public boolean allowsShowIndex() {
            return schema;
        }

        @Override
        public boolean allowsShowConstraint() {
            return schema;
        }

        @Override
        public boolean allowsTraverseAllLabels() {
            return read;
        }

        @Override
        public boolean allowsTraverseAllNodesWithLabel(int label) {
            return read;
        }

        @Override
        public boolean disallowsTraverseLabel(int label) {
            return false;
        }

        @Override
        public boolean allowsTraverseNode(int... labels) {
            return read;
        }

        @Override
        public IntSet getTraverseSecurityProperties(int[] labels) {
            return IntSets.immutable.empty();
        }

        @Override
        public boolean hasApplicableTraverseAllowPropertyRules(int label) {
            return read;
        }

        @Override
        public boolean allowsTraverseNodeWithPropertyRules(
                ReadSecurityPropertyProvider propertyProvider, int... labels) {
            return read;
        }

        @Override
        public boolean hasTraversePropertyRules() {
            return false;
        }

        @Override
        public boolean allowsTraverseAllRelTypes() {
            return read;
        }

        @Override
        public boolean allowsTraverseRelType(int relType) {
            return read;
        }

        @Override
        public boolean disallowsTraverseRelType(int relType) {
            return false;
        }

        @Override
        public boolean allowsReadPropertyAllLabels(int propertyKey) {
            return read;
        }

        @Override
        public boolean disallowsReadPropertyForSomeLabel(int propertyKey) {
            return false;
        }

        @Override
        public boolean allowsReadNodeProperties(
                Supplier<TokenSet> labels, int[] propertyKeys, ReadSecurityPropertyProvider propertyProvider) {
            return read;
        }

        @Override
        public boolean allowsReadNodeProperties(Supplier<TokenSet> labels, int[] propertyKeys) {
            return read;
        }

        @Override
        public boolean allowsReadNodeProperty(
                Supplier<TokenSet> labels, int propertyKey, ReadSecurityPropertyProvider propertyProvider) {
            return read;
        }

        @Override
        public boolean allowsReadNodeProperty(Supplier<TokenSet> labels, int propertyKey) {
            return read;
        }

        @Override
        public boolean allowsReadPropertyAllRelTypes(int propertyKey) {
            return read;
        }

        @Override
        public boolean allowsReadRelationshipProperty(RelTypeSupplier relType, int propertyKey) {
            return read;
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
        public boolean allowsSeePropertyKeyToken(int propertyKey) {
            return read;
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
        public PermissionState allowsExecuteProcedure(int procedureId) {
            return PermissionState.EXPLICIT_GRANT;
        }

        @Override
        public PermissionState shouldBoostProcedure(int procedureId) {
            return PermissionState.fromAllowList(procedureBoost);
        }

        @Override
        public PermissionState allowsExecuteFunction(int id) {
            return PermissionState.EXPLICIT_GRANT;
        }

        @Override
        public PermissionState shouldBoostFunction(int id) {
            return PermissionState.fromAllowList(procedureBoost);
        }

        @Override
        public PermissionState allowsExecuteAggregatingFunction(int id) {
            return PermissionState.EXPLICIT_GRANT;
        }

        @Override
        public PermissionState shouldBoostAggregatingFunction(int id) {
            return PermissionState.fromAllowList(procedureBoost);
        }

        @Override
        public PermissionState allowsShowSetting(String setting) {
            return PermissionState.EXPLICIT_GRANT;
        }

        @Override
        public boolean allowsSetLabel(int labelId) {
            return write;
        }

        @Override
        public boolean allowsRemoveLabel(int labelId) {
            return write;
        }

        @Override
        public boolean allowsCreateNode(int[] labelIds) {
            return write;
        }

        @Override
        public boolean allowsDeleteNode(Supplier<TokenSet> labelSupplier) {
            return write;
        }

        @Override
        public boolean allowsCreateRelationship(int relType) {
            return write;
        }

        @Override
        public boolean allowsDeleteRelationship(int relType) {
            return write;
        }

        @Override
        public boolean allowsSetProperty(Supplier<TokenSet> labels, int propertyKey) {
            return write;
        }

        @Override
        public boolean allowsSetProperty(RelTypeSupplier relType, int propertyKey) {
            return write;
        }

        @Override
        public PermissionState allowsLoadAllData() {
            return PermissionState.fromAllowList(read);
        }

        @Override
        public PermissionState allowsLoadUri(URI uri, InetAddress inetAddress) {
            return PermissionState.fromAllowList(read);
        }
    }

    boolean allowsWrites();

    PermissionState allowsTokenCreates(PrivilegeAction action);

    boolean allowsSchemaWrites();

    PermissionState allowsSchemaWrites(PrivilegeAction action);

    boolean allowsShowIndex();

    boolean allowsShowConstraint();

    /**
     * true if all nodes can be traversed
     */
    boolean allowsTraverseAllLabels();

    /**
     * true if all nodes with this label always can be traversed
     */
    boolean allowsTraverseAllNodesWithLabel(int label);

    /**
     * true if this label is deny-listed for traversal
     */
    boolean disallowsTraverseLabel(int label);

    /**
     * true if a particular node with exactly these labels can be traversed.
     *
     * @param labels the labels on the node to be checked. If labels only contains {@link org.neo4j.token.api.TokenConstants#ANY_LABEL} it will work
     *               the same as {@link #allowsTraverseAllLabels}
     */
    boolean allowsTraverseNode(int... labels);

    /**
     * Gets the keys of the operand properties (aka Security Properties) whose
     * values are to be checked by the property rules of nodes having the {@code labels} supplied
     * @param labels - the node labels which may have security rules on them
     * @return the set of operand properties
     */
    IntSet getTraverseSecurityProperties(int[] labels);

    /**
     * checks whether there is potential for nodes with this label to be traversed subject of property-based
     * GRANTS evaluating to true and not being precluded by label-based DENYs.
     * @param label - the label to check permissions for
     * @return true when nodes with this label could be traversable due to property-based GRANTS
     */
    boolean hasApplicableTraverseAllowPropertyRules(int label);

    /**
     * Uses the {@code propertyProvider} to get the node property values and the {@code labels} to get the relevant property rules,
     * and then evaluates the property rules to determine whether the node can be traversed. Also checks label-based traverse rules.
     * @param propertyProvider provider of the scrutinee node's properties
     * @param labels the labels of the node. Used to determine which property rules need to be checked.
     * @return {@code true} if traversal of this node is allowed
     */
    boolean allowsTraverseNodeWithPropertyRules(ReadSecurityPropertyProvider propertyProvider, int... labels);

    /**
     * Determines whether there are any property rules controlling traversal
     * @return {@code true} when the authenticated principal's ability to traverse nodes could be subject to property rules
     */
    boolean hasTraversePropertyRules();

    /**
     * true if all relationships can be traversed
     */
    boolean allowsTraverseAllRelTypes();

    /**
     * true if the relType can be traversed.
     *
     * @param relType the relationship type to check access for. If relType is {@link org.neo4j.token.api.TokenConstants#ANY_RELATIONSHIP_TYPE} it will work
     *                the same as {@link #allowsTraverseAllRelTypes}
     */
    boolean allowsTraverseRelType(int relType);

    /**
     * true if the relType is deny-listed for traversal.
     *
     * @param relType the relationship type to check access for.
     */
    boolean disallowsTraverseRelType(int relType);

    boolean allowsReadPropertyAllLabels(int propertyKey);

    boolean disallowsReadPropertyForSomeLabel(int propertyKey);

    /**
     * determines whether the authenticated principal is allowed to read the specified {@code propertyKeys} according
     * to the property-based RBAC read rules AND the label-based RBAC rules.
     * Optimised for a multi-property reads.
     * @param labels the labels of the node in question. Used to determine which RBAC rules are applicable.
     * @param propertyKeys the properties which the principal is requesting to read
     * @param propertyProvider the provider of the node's property values. Used as operands for the property rules.
     * @return {@code true} if the principal is allowed to read ALL of the requested {@code propertyKeys}
     */
    boolean allowsReadNodeProperties(
            Supplier<TokenSet> labels, int[] propertyKeys, ReadSecurityPropertyProvider propertyProvider);

    /**
     * determines whether the authenticated principal is allowed to read the specified {@code propertyKeys} according
     * to label-based RBAC rules. For use in contexts where there are no property-based RBAC rules in place.
     * Optimised for a multi-property reads.
     * @param labels the labels of the node in question. Used to determine which RBAC rules are applicable.
     * @param propertyKeys the properties which the principal is requesting to read
     * @return {@code true} if the principal is allowed to read ALL of the requested {@code propertyKeys}
     */
    boolean allowsReadNodeProperties(Supplier<TokenSet> labels, int[] propertyKeys);

    /**
     * determines whether the authenticated principal is allowed to read the specified {@code propertyKey} according
     * to the property-based RBAC read rules AND the label-based RBAC rules.
     * Optimised for a single-property reads.
     * @param labels the labels of the node in question. Used to determine which RBAC rules are applicable.
     * @param propertyKey the property which the principal is requesting to read
     * @param propertyProvider the provider of the node's property values. Used as operands for the property rules.
     * @return {@code true} if the principal is allowed to read  the requested {@code propertyKey}
     */
    boolean allowsReadNodeProperty(
            Supplier<TokenSet> labels, int propertyKey, ReadSecurityPropertyProvider propertyProvider);

    /**
     * determines whether the authenticated principal is allowed to read the specified {@code propertyKey} according
     * to the label-based RBAC rules. For use in contexts where there are no property-based RBAC rules.
     * Optimised for a single-property reads.
     * @param labels the labels of the node in question. Used to determine which RBAC rules are applicable.
     * @param propertyKey the property which the principal is requesting to read
     * @return {@code true} if the principal is allowed to read  the requested {@code propertyKey}
     */
    boolean allowsReadNodeProperty(Supplier<TokenSet> labels, int propertyKey);

    boolean allowsReadPropertyAllRelTypes(int propertyKey);

    boolean allowsReadRelationshipProperty(RelTypeSupplier relType, int propertyKey);

    boolean allowsSeePropertyKeyToken(int propertyKey);

    /**
     * Determines whether there are any property rules controlling the ability to read properties.
     * @return {@code true} when the authenticated principal's ability to read node properties could be subject to property rules
     */
    boolean hasPropertyReadRules();

    /**
     * Determines whether there are any property rules controlling the ability to read the specified {@code propertyKeys}.
     * @return {@code true} when the authenticated principal's ability to read any of the specified {@code propertyKeys}
     * could be subject to property rules (further dependent on the labels of the node in question).
     */
    boolean hasPropertyReadRules(int... propertyKeys);

    /**
     * Get the keys of the properties which are used as operands for rules controlling the ability to read {@code propertyKey}
     * @param propertyKey the key of the property whose reading is being restricted
     * @return the list of keys of the properties which will be scrutinised in determining whether {@code propertyKey} can be read
     */
    IntSet getReadSecurityProperties(int propertyKey);

    /**
     * Get all keys of the properties which are used as operands for rules controlling the ability to read certain properties
     * @return the list of keys of the properties which will be scrutinised in determining whether certain properties can be read
     */
    IntSet getAllReadSecurityProperties();

    /**
     * Given a PropertySelection get the PropertySelection for the corresponding security properties
     * @param selection the properties to get the security properties for
     * @return the security properties which are operands to the relevant property rules
     */
    PropertySelection getSecurityPropertySelection(PropertySelection selection);

    /**
     * Check if execution of a procedure is allowed
     *
     * @param procedureId id of the procedure
     * @return true if the procedure with this id is allowed to be executed
     */
    PermissionState allowsExecuteProcedure(int procedureId);

    /**
     * Check if execution of a procedure should be done with boosted privileges.
     * <p>
     * <strong>Note: this does not check if execution is allowed</strong>
     *
     * @param procedureId id of the procedure
     * @return true if the procedure with this id should be executed with boosted privileges
     */
    PermissionState shouldBoostProcedure(int procedureId);

    /**
     * Check if execution of a user defined function is allowed
     *
     * @param id id of the function
     * @return true if the function with this id is allowed to be executed
     */
    PermissionState allowsExecuteFunction(int id);

    /**
     * Check if execution of a user defined function should be done with boosted privileges.
     * <p>
     * <strong>Note: this does not check if execution is allowed</strong>
     *
     * @param id id of the function
     * @return true if the function with this id should be executed with boosted privileges
     */
    PermissionState shouldBoostFunction(int id);

    /**
     * Check if execution of a aggregating user defined function is allowed
     *
     * @param id id of the function
     * @return true if the function with this id is allowed to be executed
     */
    PermissionState allowsExecuteAggregatingFunction(int id);

    /**
     * Check if execution of a aggregating user defined function should be done with boosted privileges.
     * <p>
     * <strong>Note: this does not check if execution is allowed</strong>
     *
     * @param id id of the function
     * @return true if the function with this id should be executed with boosted privileges
     */
    PermissionState shouldBoostAggregatingFunction(int id);

    /**
     * Check if a given setting is available to the executing user
     *
     * @param setting name of the setting
     * @return true if the setting is available to user
     */
    PermissionState allowsShowSetting(String setting);

    boolean allowsSetLabel(int labelId);

    boolean allowsRemoveLabel(int labelId);

    boolean allowsCreateNode(int[] labelIds);

    boolean allowsDeleteNode(Supplier<TokenSet> labelSupplier);

    boolean allowsCreateRelationship(int relType);

    boolean allowsDeleteRelationship(int relType);

    boolean allowsSetProperty(Supplier<TokenSet> labels, int propertyKey);

    boolean allowsSetProperty(RelTypeSupplier relType, int propertyKey);

    PermissionState allowsLoadAllData();

    PermissionState allowsLoadUri(URI url, InetAddress inetAddress);

    String name();

    default Set<String> roles() {
        return Collections.emptySet();
    }

    default boolean isOverridden() {
        return false;
    }

    default boolean isCacheable() {
        return false;
    }
}
