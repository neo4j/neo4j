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
package org.neo4j.shell.cli;

import static org.neo4j.shell.Main.EXIT_FAILURE;
import static org.neo4j.shell.Main.EXIT_SUCCESS;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import org.neo4j.shell.Historian;
import org.neo4j.shell.ShellRunner;
import org.neo4j.shell.StatementExecuter;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parser.StatementParser;
import org.neo4j.shell.printer.Printer;

/**
 * A shell runner which reads all of STDIN and executes commands until completion. In case of errors, the failBehavior determines if the shell exits
 * immediately, or if it should keep trying the next commands.
 */
public class NonInteractiveShellRunner implements ShellRunner {
    private static final Logger log = Logger.create();
    private final FailBehavior failBehavior;
    private final StatementExecuter executer;
    private final Printer printer;
    private final StatementParser statementParser;
    private final InputStream inputStream;

    public NonInteractiveShellRunner(
            FailBehavior failBehavior,
            StatementExecuter executer,
            Printer printer,
            StatementParser statementParser,
            InputStream inputStream) {
        this.failBehavior = failBehavior;
        this.executer = executer;
        this.printer = printer;
        this.statementParser = statementParser;
        this.inputStream = inputStream;
    }

    @Override
    public int runUntilEnd() {
        List<StatementParser.ParsedStatement> statements;
        try (Reader reader = new InputStreamReader(inputStream)) {
            statements = statementParser.parse(reader).statements();
        } catch (Throwable e) {
            log.error(e);
            printer.printError(e);
            return EXIT_FAILURE;
        }

        int exitCode = EXIT_SUCCESS;

        for (var statement : statements) {
            try {
                executer.execute(statement);
            } catch (ExitException e) {
                log.info("ExitException code=" + e.getCode() + ": " + e.getMessage());
                // These exceptions are always fatal
                return e.getCode();
            } catch (Throwable e) {
                exitCode = EXIT_FAILURE;
                log.error(e);
                printer.printError(e);
                if (FailBehavior.FAIL_AT_END != failBehavior) {
                    return exitCode;
                }
            }
        }
        return exitCode;
    }

    @Override
    public Historian getHistorian() {
        return Historian.empty;
    }

    @Override
    public boolean isInteractive() {
        return false;
    }
}
