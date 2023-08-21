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

import java.util.List;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;

/**
 * This command marks a transaction as failed and closes it.
 */
public class Rollback implements Command {
    private final TransactionHandler transactionHandler;

    public Rollback(final TransactionHandler transactionHandler) {
        this.transactionHandler = transactionHandler;
    }

    @Override
    public void execute(final List<String> args) throws ExitException, CommandException {
        requireArgumentCount(args, 0);
        transactionHandler.rollbackTransaction();
    }

    public static class Factory implements Command.Factory {
        @Override
        public Metadata metadata() {
            var help = "Roll back and closes the currently open transaction";
            return new Metadata(":rollback", "Rollback the currently open transaction", "", help, List.of());
        }

        @Override
        public Command executor(Arguments args) {
            return new Rollback(args.cypherShell());
        }
    }
}
