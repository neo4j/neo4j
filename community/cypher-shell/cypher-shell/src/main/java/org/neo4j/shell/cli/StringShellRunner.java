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

import javax.annotation.Nonnull;

import org.neo4j.shell.Historian;
import org.neo4j.shell.ShellRunner;
import org.neo4j.shell.StatementExecuter;
import org.neo4j.shell.log.Logger;

/**
 * A shell runner which executes a single String and exits afterward. Any errors will throw immediately.
 */
public class StringShellRunner implements ShellRunner
{
    private final String cypher;
    private final Logger logger;
    private final StatementExecuter executer;

    public StringShellRunner( @Nonnull CliArgs cliArgs,
                              @Nonnull StatementExecuter executer,
                              @Nonnull Logger logger )
    {
        this.executer = executer;
        this.logger = logger;
        if ( cliArgs.isStringShell() )
        {
            this.cypher = cliArgs.getCypher().get();
        }
        else
        {
            throw new NullPointerException( "No cypher string specified" );
        }
    }

    @Override
    public int runUntilEnd()
    {
        int exitCode = 0;
        try
        {
            executer.execute( cypher.trim() );
        }
        catch ( Throwable t )
        {
            logger.printError( t );
            exitCode = 1;
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
