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
package org.neo4j.kernel.impl.util;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;

public class TransactionLogChecker {
    private TransactionLogChecker() {}

    /**
     * Should eventually check that the transaction log content and headers are correct in respect to
     * version changes and rotations.
     */
    public static void verifyCorrectTransactionLogUpgrades(FileSystemAbstraction fs, DatabaseLayout layout) {
        // Placeholder. Logic to be added that will verify that
        // any kernel upgrades have triggered correct behavior in
        // the transaction log.
    }
}
