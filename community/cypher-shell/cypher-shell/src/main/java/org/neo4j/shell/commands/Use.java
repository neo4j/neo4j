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
package org.neo4j.shell.commands;

import static org.neo4j.shell.commands.CommandHelper.stripEnclosingBackTicks;

import java.util.List;
import org.neo4j.shell.DatabaseManager;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;

/**
 * This command starts a transaction.
 */
public class Use implements Command {
    private final DatabaseManager databaseManager;

    public Use(final DatabaseManager databaseManager) {
        this.databaseManager = databaseManager;
    }

    @Override
    public void execute(final List<String> args) throws ExitException, CommandException {
        requireArgumentCount(args, 0, 1);
        String databaseName = args.size() == 0 ? DatabaseManager.ABSENT_DB_NAME : args.get(0);
        databaseManager.setActiveDatabase(stripEnclosingBackTicks(databaseName));
    }

    public static class Factory implements Command.Factory {
        @Override
        public Metadata metadata() {
            var help = "Set the active database that transactions are executed on";
            return new Metadata(":use", "Set the active database", "database", help, List.of());
        }

        @Override
        public Command executor(Arguments args) {
            return new Use(args.cypherShell());
        }
    }
}
