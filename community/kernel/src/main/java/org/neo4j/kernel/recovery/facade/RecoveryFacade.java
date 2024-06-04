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
package org.neo4j.kernel.recovery.facade;

import java.io.IOException;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.recovery.RecoveryMode;

public interface RecoveryFacade {
    RecoveryFacade EMPTY = EmptyRecoveryFacade.INSTANCE;

    void performRecovery(
            DatabaseLayout layout,
            RecoveryCriteria recoveryCriteria,
            RecoveryFacadeMonitor recoveryFacadeMonitor,
            RecoveryMode recoveryMode,
            boolean rollbackIncompleteTransactions)
            throws IOException;

    void performRecovery(DatabaseLayout databaseLayout) throws IOException;

    void performRecovery(DatabaseLayout databaseLayout, RecoveryFacadeMonitor monitor, RecoveryMode mode)
            throws IOException;

    void performRecovery(
            DatabaseLayout databaseLayout,
            RecoveryCriteria recoveryCriteria,
            RecoveryFacadeMonitor monitor,
            boolean recoverOnlyAvailableTransactions)
            throws IOException;

    void forceRecovery(
            DatabaseLayout databaseLayout,
            RecoveryFacadeMonitor monitor,
            RecoveryMode recoveryMode,
            boolean rollbackIncompleteTransactions)
            throws IOException;
}
