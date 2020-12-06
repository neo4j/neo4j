/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.shell.cli;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import javax.annotation.Nonnull;

import org.neo4j.shell.Historian;
import org.neo4j.shell.ShellRunner;
import org.neo4j.shell.StatementExecuter;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parser.StatementParser;

import static org.neo4j.shell.Main.EXIT_FAILURE;
import static org.neo4j.shell.Main.EXIT_SUCCESS;

/**
 * A shell runner which reads all of STDIN and executes commands until completion. In case of errors, the failBehavior determines if the shell exits
 * immediately, or if it should keep trying the next commands.
 */
public class NonInteractiveShellRunner implements ShellRunner
{

    private final FailBehavior failBehavior;
    @Nonnull
    private final StatementExecuter executer;
    private final Logger logger;
    private final StatementParser statementParser;
    private final InputStream inputStream;

    public NonInteractiveShellRunner( @Nonnull FailBehavior failBehavior,
                                      @Nonnull StatementExecuter executer,
                                      @Nonnull Logger logger,
                                      @Nonnull StatementParser statementParser,
                                      @Nonnull InputStream inputStream )
    {
        this.failBehavior = failBehavior;
        this.executer = executer;
        this.logger = logger;
        this.statementParser = statementParser;
        this.inputStream = inputStream;
    }

    @Override
    public int runUntilEnd()
    {
        List<String> statements;
        try ( BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( inputStream ) ) )
        {
            bufferedReader
                    .lines()
                    .forEach( line -> statementParser.parseMoreText( line + "\n" ) );
            statements = statementParser.consumeStatements();
        }
        catch ( Throwable e )
        {
            logger.printError( e );
            return 1;
        }

        int exitCode = EXIT_SUCCESS;
        for ( String statement : statements )
        {
            try
            {
                executer.execute( statement );
            }
            catch ( ExitException e )
            {
                // These exceptions are always fatal
                return e.getCode();
            }
            catch ( Throwable e )
            {
                exitCode = EXIT_FAILURE;
                logger.printError( e );
                if ( FailBehavior.FAIL_AT_END != failBehavior )
                {
                    return exitCode;
                }
            }
        }
        return exitCode;
    }

    @Nonnull
    @Override
    public Historian getHistorian()
    {
        return Historian.empty;
    }
}
