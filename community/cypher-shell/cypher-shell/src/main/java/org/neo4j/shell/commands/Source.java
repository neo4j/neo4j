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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.parser.ShellStatementParser;
import org.neo4j.shell.parser.StatementParser;

/**
 * This command reads a cypher file frome the filesystem and executes the statements therein.
 */
public class Source implements Command {
    private final CypherShell cypherShell;
    private final StatementParser statementParser;

    public Source(CypherShell cypherShell, StatementParser statementParser) {
        this.cypherShell = cypherShell;
        this.statementParser = statementParser;
    }

    @Override
    public void execute(final List<String> args) throws ExitException, CommandException {
        requireArgumentCount(args, 1);
        String filename = args.get(0);

        try (Reader reader = new InputStreamReader(new FileInputStream(filename))) {
            cypherShell.execute(statementParser.parse(reader).statements());
        } catch (IOException e) {
            throw new CommandException(format("Cannot find file: '%s'", filename), e);
        }
    }

    public static class Factory implements Command.Factory {
        @Override
        public Metadata metadata() {
            var description = "Executes Cypher statements from a file";
            var help = "Executes Cypher statements from a file";
            return new Metadata(":source", description, "[filename]", help, List.of());
        }

        @Override
        public Command executor(Arguments args) {
            return new Source(args.cypherShell(), new ShellStatementParser());
        }
    }
}
