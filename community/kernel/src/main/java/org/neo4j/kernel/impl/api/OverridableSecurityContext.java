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

import org.neo4j.internal.kernel.api.security.SecurityContext;

/**
 * Procedures and functions can be executed with boosted or restricted security context.
 * This creates the need for temporarily changing the security context and then return to the original one.
 */
public class OverridableSecurityContext {

    private final SecurityContext originalSecurityContext;
    private SecurityContext currentSecurityContext;

    OverridableSecurityContext(SecurityContext securityContext) {
        this.originalSecurityContext = securityContext;
        this.currentSecurityContext = securityContext;
    }

    /**
     * Get the security context not influenced by calls to {@link #overrideWith(SecurityContext)}.
     */
    SecurityContext originalSecurityContext() {
        return originalSecurityContext;
    }

    public SecurityContext currentSecurityContext() {
        return currentSecurityContext;
    }

    /**
     * Temporarily override this  SecurityContext. The override should be reverted using
     * the returned {@link Revertable}.
     *
     * @param context the temporary SecurityContext.
     * @return {@link Revertable} which reverts to the SecurityContext before the override.
     */
    public Revertable overrideWith(SecurityContext context) {
        SecurityContext contextBeforeOverride = currentSecurityContext;
        currentSecurityContext = context;
        return () -> currentSecurityContext = contextBeforeOverride;
    }

    @FunctionalInterface
    public interface Revertable extends AutoCloseable {
        @Override
        void close();
    }
}
