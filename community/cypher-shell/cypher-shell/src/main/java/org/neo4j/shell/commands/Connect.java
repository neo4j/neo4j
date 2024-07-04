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

import static java.lang.String.format;
import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;
import static org.neo4j.shell.cli.CliArgHelper.DATABASE_ENV_VAR;

import java.util.List;
import java.util.Optional;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.Environment;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.exception.NoMoreInputException;
import org.neo4j.shell.exception.UserInterruptException;
import org.neo4j.shell.terminal.CypherShellTerminal;

/**
 * Connects to a database
 */
public class Connect implements Command {
    private final CypherShell shell;
    private final CypherShellTerminal terminal;
    private final ArgumentParser argumentParser;
    private final Environment environment;

    public Connect(CypherShell shell, CypherShellTerminal terminal, Environment environment) {
        this.shell = shell;
        this.terminal = terminal;
        this.argumentParser = setupParser();
        this.environment = environment;
    }

    @Override
    public void execute(final List<String> args) throws ExitException, CommandException {
        if (shell.isConnected()) {
            throw new CommandException("Already connected");
        }

        connect(args);
    }

    private void connect(List<String> stringArgs) throws CommandException {
        requireArgumentCount(stringArgs, 0, 6);
        try {
            var args = argumentParser.parseArgs(stringArgs.toArray(new String[0]));
            var user = args.getString("username");
            var password = args.getString("password");

            if (user == null && password != null) {
                throw new CommandException(
                        "You cannot provide password only, please provide a username using '-u USERNAME'");
            } else if (user == null) // We know password is null because of the previous if statement
            {
                user = promptForNonEmptyText("username", false);
                password = promptForText("password", true);
            } else if (password == null) {
                password = promptForText("password", true);
            }

            shell.connect(user, password, database(args));
        } catch (ArgumentParserException e) {
            throw new CommandException(format(
                    "Invalid input string: '%s', usage: ':connect %s'",
                    String.join(" ", stringArgs), metadata().usage()));
        }
    }

    private ArgumentParser setupParser() {
        var parser = ArgumentParsers.newFor(metadata().name()).build();
        parser.addArgument("-d", "--database");
        parser.addArgument("-u", "--username");
        parser.addArgument("-p", "--password");
        return parser;
    }

    private String promptForNonEmptyText(String prompt, boolean mask) throws CommandException {
        String text = promptForText(prompt, mask);
        if (!text.isEmpty()) {
            return text;
        }
        terminal.write().println(prompt + " cannot be empty");
        terminal.write().println();
        return promptForNonEmptyText(prompt, mask);
    }

    private String promptForText(String prompt, boolean mask) throws CommandException {
        try {
            return terminal.read().simplePrompt(prompt + ": ", mask);
        } catch (NoMoreInputException | UserInterruptException e) {
            throw new CommandException("No text could be read, exiting...");
        }
    }

    private String database(Namespace ns) {
        return Optional.ofNullable(ns.getString("database"))
                .or(() -> Optional.ofNullable(environment.getVariable(DATABASE_ENV_VAR)))
                .orElse(ABSENT_DB_NAME);
    }

    public static class Factory implements Command.Factory {
        @Override
        public Metadata metadata() {
            var usage =
                    "[-u USERNAME, --username USERNAME], [-p PASSWORD, --password PASSWORD], [-d DATABASE, --database DATABASE]";
            var help = format(":connect %s, connects to a database", usage);
            return new Metadata(":connect", "Connects to a database", usage, help, List.of());
        }

        @Override
        public Command executor(Arguments args) {
            return new Connect(args.cypherShell(), args.terminal(), new Environment());
        }
    }
}
