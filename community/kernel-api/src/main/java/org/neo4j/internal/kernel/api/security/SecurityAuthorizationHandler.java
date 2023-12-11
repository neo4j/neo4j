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

import static java.lang.String.format;

import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.messages.MessageUtil;

/**
 * can be used for authorization and security logging
 */
public class SecurityAuthorizationHandler {
    AbstractSecurityLog securityLog;

    public SecurityAuthorizationHandler(AbstractSecurityLog securityLog) {
        this.securityLog = securityLog;
    }

    public void assertAllowsCreateNode(SecurityContext securityContext, IntFunction<String> resolver, int[] labelIds) {
        AccessMode accessMode = securityContext.mode();
        if (!accessMode.allowsCreateNode(labelIds)) {
            String labels = null == labelIds
                    ? ""
                    : Arrays.stream(labelIds).mapToObj(resolver).collect(Collectors.joining(","));
            throw logAndGetAuthorizationException(
                    securityContext,
                    MessageUtil.createNodeWithLabelsDenied(
                            labels, securityContext.database(), securityContext.description()));
        }
    }

    public void assertAllowsDeleteNode(
            SecurityContext securityContext, IntFunction<String> resolver, Supplier<TokenSet> labelSupplier) {
        AccessMode accessMode = securityContext.mode();
        if (!accessMode.allowsDeleteNode(labelSupplier)) {
            String labels =
                    Arrays.stream(labelSupplier.get().all()).mapToObj(resolver).collect(Collectors.joining(","));
            throw logAndGetAuthorizationException(
                    securityContext,
                    format(
                            "Delete node with labels '%s' on database '%s' is not allowed for %s.",
                            labels, securityContext.database(), securityContext.description()));
        }
    }

    public void assertAllowsCreateRelationship(
            SecurityContext securityContext, IntFunction<String> resolver, int relType) {
        AccessMode accessMode = securityContext.mode();
        if (!accessMode.allowsCreateRelationship(relType)) {
            throw logAndGetAuthorizationException(
                    securityContext,
                    format(
                            "Create relationship with type '%s' on database '%s' is not allowed for %s.",
                            resolver.apply(relType), securityContext.database(), securityContext.description()));
        }
    }

    public void assertAllowsDeleteRelationship(
            SecurityContext securityContext, IntFunction<String> resolver, int relType) {
        AccessMode accessMode = securityContext.mode();
        if (!accessMode.allowsDeleteRelationship(relType)) {
            throw logAndGetAuthorizationException(
                    securityContext,
                    format(
                            "Delete relationship with type '%s' on database '%s' is not allowed for %s.",
                            resolver.apply(relType), securityContext.database(), securityContext.description()));
        }
    }

    public void assertAllowsSetLabel(SecurityContext securityContext, IntFunction<String> resolver, int labelId) {
        AccessMode accessMode = securityContext.mode();
        if (!accessMode.allowsSetLabel(labelId)) {
            throw logAndGetAuthorizationException(
                    securityContext,
                    format(
                            "Set label for label '%s' on database '%s' is not allowed for %s.",
                            resolver.apply(labelId), securityContext.database(), securityContext.description()));
        }
    }

    public void assertAllowsRemoveLabel(SecurityContext securityContext, IntFunction<String> resolver, int labelId) {
        AccessMode accessMode = securityContext.mode();
        if (!accessMode.allowsRemoveLabel(labelId)) {
            throw logAndGetAuthorizationException(
                    securityContext,
                    format(
                            "Remove label for label '%s' on database '%s' is not allowed for %s.",
                            resolver.apply(labelId), securityContext.database(), securityContext.description()));
        }
    }

    public void assertAllowsSetProperty(
            SecurityContext securityContext, IntFunction<String> resolver, TokenSet labelIds, int propertyKey) {
        AccessMode accessMode = securityContext.mode();
        if (!accessMode.allowsSetProperty(() -> labelIds, propertyKey)) {
            throw logAndGetAuthorizationException(
                    securityContext,
                    format(
                            "Set property for property '%s' on database '%s' is not allowed for %s.",
                            resolver.apply(propertyKey), securityContext.database(), securityContext.description()));
        }
    }

    public void assertAllowsSetProperty(
            SecurityContext securityContext, IntFunction<String> resolver, long relType, int propertyKey) {
        AccessMode accessMode = securityContext.mode();
        if (!accessMode.allowsSetProperty(() -> (int) relType, propertyKey)) {
            throw logAndGetAuthorizationException(
                    securityContext,
                    format(
                            "Set property for property '%s' on database '%s' is not allowed for %s.",
                            resolver.apply(propertyKey), securityContext.database(), securityContext.description()));
        }
    }

    public void assertSchemaWrites(SecurityContext securityContext, PrivilegeAction action) {
        AccessMode accessMode = securityContext.mode();
        switch (accessMode.allowsSchemaWrites(action)) {
            case NOT_GRANTED:
                throw logAndGetAuthorizationException(
                        securityContext,
                        format(
                                "Schema operation '%s' on database '%s' is not allowed for %s.",
                                action, securityContext.database(), securityContext.description()));
            case EXPLICIT_DENY:
                throw logAndGetAuthorizationException(
                        securityContext,
                        format(
                                "Schema operation '%s' on database '%s' is denied for %s.",
                                action, securityContext.database(), securityContext.description()));
            default:
                // All is well
        }
    }

    public void assertShowIndexAllowed(SecurityContext securityContext) {
        AccessMode accessMode = securityContext.mode();
        if (!accessMode.allowsShowIndex()) {
            throw logAndGetAuthorizationException(
                    securityContext,
                    format(
                            "Show indexes on database '%s' is not allowed for %s.",
                            securityContext.database(), securityContext.description()));
        }
    }

    public void assertShowConstraintAllowed(SecurityContext securityContext) {
        AccessMode accessMode = securityContext.mode();
        if (!accessMode.allowsShowConstraint()) {
            throw logAndGetAuthorizationException(
                    securityContext,
                    format(
                            "Show constraints on database '%s' is not allowed for %s.",
                            securityContext.database(), securityContext.description()));
        }
    }

    public final void assertAllowsTokenCreates(SecurityContext securityContext, PrivilegeAction action) {
        AccessMode accessMode = securityContext.mode();
        PermissionState permissionState = accessMode.allowsTokenCreates(action);
        if (!permissionState.allowsAccess()) {
            String errorDescriptor = permissionState == PermissionState.NOT_GRANTED ? "not allowed" : "denied";
            switch (action) {
                case CREATE_LABEL:
                    throw logAndGetAuthorizationException(
                            securityContext,
                            format(
                                    "Creating new node label on database '%s' is %s for %s. "
                                            + "See GRANT CREATE NEW NODE LABEL ON DATABASE `%s`...",
                                    securityContext.database(),
                                    errorDescriptor,
                                    securityContext.description(),
                                    securityContext.database()));
                case CREATE_PROPERTYKEY:
                    throw logAndGetAuthorizationException(
                            securityContext,
                            format(
                                    "Creating new property name on database '%s' is %s for %s. "
                                            + "See GRANT CREATE NEW PROPERTY NAME ON DATABASE `%s`...",
                                    securityContext.database(),
                                    errorDescriptor,
                                    securityContext.description(),
                                    securityContext.database()));
                case CREATE_RELTYPE:
                    throw logAndGetAuthorizationException(
                            securityContext,
                            format(
                                    "Creating new relationship type on database '%s' is %s for %s. "
                                            + "See GRANT CREATE NEW RELATIONSHIP TYPE ON DATABASE `%s`...",
                                    securityContext.database(),
                                    errorDescriptor,
                                    securityContext.description(),
                                    securityContext.database()));
                default:
                    throw logAndGetAuthorizationException(
                            securityContext,
                            format(
                                    "'%s' operations on database '%s' are %s for %s.",
                                    action,
                                    securityContext.database(),
                                    errorDescriptor,
                                    securityContext.description()));
            }
        }
    }

    public void assertLoadAllowed(SecurityContext securityContext, URI uri, InetAddress inetAddress) {
        AccessMode accessMode = securityContext.mode();
        PermissionState permissionState = accessMode.allowsLoadAllData();
        if (permissionState == PermissionState.NOT_GRANTED) {
            permissionState = accessMode.allowsLoadUri(uri, inetAddress);
        } else if (permissionState == PermissionState.EXPLICIT_GRANT) {
            permissionState = permissionState.combine(accessMode.allowsLoadUri(uri, inetAddress));
        }

        if (!permissionState.allowsAccess()) {
            String errorDescriptor = permissionState == PermissionState.NOT_GRANTED ? "not allowed" : "denied";
            throw logAndGetAuthorizationException(
                    securityContext,
                    format("LOAD on URL '%s' is %s for %s.", uri, errorDescriptor, securityContext.description()));
        }
    }

    public AuthorizationViolationException logAndGetAuthorizationException(
            SecurityContext securityContext, String message) {
        securityLog.error(securityContext, message);
        return new AuthorizationViolationException(message);
    }

    public AuthorizationViolationException logAndGetAuthorizationException(
            SecurityContext securityContext, String message, Status status) {
        securityLog.error(securityContext, message);
        return new AuthorizationViolationException(message, status);
    }

    public static String generateCredentialsExpiredMessage(String message) {
        return format(
                "%s%n%nThe credentials you provided were valid, but must be changed before you can use this instance. "
                        + "If this is the first time you are using Neo4j, this is to ensure you are not using the default credentials in production. "
                        + "If you are not using default credentials, you are getting this message because an administrator requires a password change.%n"
                        + "To change your password, issue an `ALTER CURRENT USER SET PASSWORD FROM 'current password' TO 'new password'` "
                        + "statement against the system database.",
                message);
    }
}
