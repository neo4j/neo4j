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
package org.neo4j.kernel.impl.api.security;

import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.PermissionState;

/**
 * Access mode that wraps an access mode with a wrapping access mode. The resulting access mode allows things based
 * on both the original and the wrapping mode, while retaining the meta data of the original mode only.
 */
abstract class WrappedAccessMode implements AccessMode {
    protected final AccessMode original;
    protected final Static wrapping;

    WrappedAccessMode(AccessMode original, Static wrapping) {
        this.original = original;
        if (original instanceof WrappedAccessMode) {
            Static originalWrapping = ((WrappedAccessMode) original).wrapping;
            this.wrapping = originalWrapping.ordinal() < wrapping.ordinal() ? originalWrapping : wrapping;
        } else {
            this.wrapping = wrapping;
        }
    }

    @Override
    public PermissionState allowsExecuteProcedure(int procedureId) {
        return original.allowsExecuteProcedure(procedureId);
    }

    @Override
    public PermissionState allowExecuteAdminProcedures() {
        return original.allowExecuteAdminProcedures();
    }

    @Override
    public PermissionState shouldBoostProcedure(int procedureId) {
        return original.shouldBoostProcedure(procedureId);
    }

    @Override
    public PermissionState allowsExecuteFunction(int id) {
        return original.allowsExecuteFunction(id);
    }

    @Override
    public PermissionState shouldBoostFunction(int id) {
        return original.shouldBoostFunction(id);
    }

    @Override
    public PermissionState allowsExecuteAggregatingFunction(int id) {
        return original.allowsExecuteAggregatingFunction(id);
    }

    @Override
    public PermissionState shouldBoostAggregatingFunction(int id) {
        return original.shouldBoostFunction(id);
    }

    @Override
    public boolean isOverridden() {
        return true;
    }
}
