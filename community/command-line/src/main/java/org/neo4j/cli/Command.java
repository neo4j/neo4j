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
package org.neo4j.cli;

import java.util.concurrent.Callable;

public interface Command extends Callable<Integer>
{
    //NOTE! The order of declaration here is the order they get listed in the CLI. Please keep it grouped by area of usage.
    enum CommandType
    {
        CHECK_CONSISTENCY,
        DIAGNOSTICS_REPORT,
        STORE_INFO,
        MEMORY_RECOMMENDATION,
        IMPORT,
        STORE_COPY,
        SET_DEFAULT_ADMIN,
        SET_INITIAL_PASSWORD,
        SET_OPERATOR_PASSWORD,
        DUMP,
        LOAD,
        ONLINE_BACKUP,
        RESTORE_DB,
        PREPARE_RESTORE,
        UNBIND,
        PUSH_TO_CLOUD
    }
}
