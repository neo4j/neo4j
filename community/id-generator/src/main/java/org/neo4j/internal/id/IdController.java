/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.id;

import java.util.function.Supplier;

import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Represent abstraction that responsible for any id related operations on a storage engine level: buffering,
 * maintenance, clearing, resetting, generation.
 */
public interface IdController extends Lifecycle
{
    /**
     * Essentially a condition to check whether or not the {@link IdController} can free a batch of IDs, in maintenance.
     * For a concrete example it can be a snapshot of ongoing transactions. Then given that snapshot {@link #eligibleForFreeing()}
     * would check whether or not all of those transactions from the snapshot were closed.
     */
    interface IdFreeCondition
    {
        /**
         * @return whether or not the condition for freeing has been met so that maintenance can free a specific batch of ids.
         */
        boolean eligibleForFreeing();
    }

    /**
     * Perform ids related maintenance.
     */
    void maintenance();

    void initialize( Supplier<IdFreeCondition> conditionSupplier );
}
