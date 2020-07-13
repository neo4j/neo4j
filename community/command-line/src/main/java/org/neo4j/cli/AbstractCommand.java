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
package org.neo4j.cli;

import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.io.PrintStream;

import org.neo4j.kernel.diagnostics.providers.SystemDiagnostics;
import org.neo4j.kernel.internal.Version;

import static java.util.Objects.requireNonNull;

@CommandLine.Command(
        headerHeading = "%n",
        synopsisHeading = "%n@|bold,underline USAGE|@%n%n",
        descriptionHeading = "%n@|bold,underline DESCRIPTION|@%n%n",
        optionListHeading = "%n@|bold,underline OPTIONS|@%n%n",
        parameterListHeading = "%n@|bold,underline PARAMETERS|@%n%n",
        showDefaultValues = true,
        sortOptions = false
)
public abstract class AbstractCommand implements Command
{
    @Option( names = "--verbose", arity = "0", description = "Enable verbose output." )
    protected boolean verbose;

    protected final ExecutionContext ctx;
    @Spec
    protected CommandSpec spec;

    protected AbstractCommand( ExecutionContext ctx )
    {
        this.ctx = requireNonNull( ctx );
    }

    protected abstract void execute() throws Exception;

    @Override
    public Integer call() throws Exception
    {
        if ( verbose )
        {
            printVerboseHeader();
        }
        try
        {
            execute();
        }
        catch ( CommandFailedException e )
        {
            if ( verbose )
            {
                e.printStackTrace( ctx.err() );
            }
            else
            {
                ctx.err().println( e.getMessage() );
            }
            return e.getExitCode();
        }
        return 0;
    }

    private void printVerboseHeader()
    {
        PrintStream out = ctx.out();
        out.println( "neo4j " + Version.getNeo4jVersion() );
        SystemDiagnostics.JAVA_VIRTUAL_MACHINE.dump( out::println );
    }
}
