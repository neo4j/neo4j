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
package org.neo4j.cli;

import static org.neo4j.cli.CommandGroup.DATABASE;
import static org.neo4j.cli.CommandGroup.DBMS;
import static org.neo4j.cli.CommandGroup.SERVER;

public enum CommandType {
    CHECK(DATABASE),
    INFO(DATABASE),
    IMPORT(DATABASE),
    COPY(DATABASE),
    DUMP(DATABASE),
    LOAD(DATABASE),
    BACKUP(DATABASE),
    BACKUP_NEXT(DATABASE),
    BACKUP_SNAPSHOT(DATABASE),
    INSPECT_BACKUP(DATABASE),
    RESTORE(DATABASE),
    RESTORE_NEXT(DATABASE),
    PREPARE_RESTORE(DATABASE),
    MIGRATE_STORE(DATABASE),
    UPLOAD(DATABASE),
    AGGREGATE_NEXT(DATABASE),

    REPORT(SERVER),
    MEMORY_RECOMMENDATION(SERVER),
    UNBIND(SERVER),
    NEO4J_CONSOLE(SERVER),
    GET_SERVER_ID(SERVER),
    NEO4J_START(SERVER),
    NEO4J_VALIDATE_CONFIG(SERVER),
    NEO4J_RESTART(SERVER),
    NEO4J_STATUS(SERVER),
    NEO4J_STOP(SERVER),
    NEO4J_SERVICE(SERVER),
    CONFIG_MIGRATION(SERVER),
    ACCEPT_LICENSE(SERVER),

    SET_DEFAULT_ADMIN(DBMS),
    SET_INITIAL_PASSWORD(DBMS),
    SET_OPERATOR_PASSWORD(DBMS),
    UNBIND_SYSTEM(DBMS),
    TEST(DBMS); // Used by test commands. Don't use this for any real command

    private final CommandGroup commandGroup;

    CommandType(CommandGroup commandGroup) {
        this.commandGroup = commandGroup;
    }

    public CommandGroup getCommandGroup() {
        return commandGroup;
    }
}
