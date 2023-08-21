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

/**
 * Represent the result of a permission check but provide more detail than a simple boolean Also allows for results to be combined such that denies override
 * grants and grants override nothing
 */
public enum PermissionState {
    NOT_GRANTED,
    EXPLICIT_GRANT,
    EXPLICIT_DENY;

    public PermissionState combine(PermissionState p) {
        int order = this.compareTo(p);
        if (order <= 0) {
            return p;
        } else {
            return this;
        }
    }

    public PermissionState restrict(PermissionState restricting) {
        // If both allow access, allow access
        if (this.allowsAccess() && restricting.allowsAccess()) {
            return PermissionState.EXPLICIT_GRANT;
        }
        // Else return worst case
        if (this == EXPLICIT_DENY || restricting == EXPLICIT_DENY) {
            return EXPLICIT_DENY;
        } else {
            return NOT_GRANTED;
        }
    }

    public boolean allowsAccess() {
        return this == EXPLICIT_GRANT;
    }

    public static PermissionState fromAllowList(boolean permitted) {
        if (permitted) {
            return EXPLICIT_GRANT;
        } else {
            return NOT_GRANTED;
        }
    }

    public static PermissionState fromDenyList(boolean permitted) {
        if (permitted) {
            return NOT_GRANTED;
        } else {
            return EXPLICIT_DENY;
        }
    }
}
