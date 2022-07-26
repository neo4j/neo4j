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

import static java.util.Objects.requireNonNull;

import java.io.PrintStream;
import java.util.concurrent.Callable;
import org.neo4j.kernel.diagnostics.providers.SystemDiagnostics;
import org.neo4j.kernel.internal.Version;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@CommandLine.Command(
        headerHeading = "%n",
        synopsisHeading = "%n@|bold,underline USAGE|@%n%n",
        descriptionHeading = "%n@|bold,underline DESCRIPTION|@%n%n",
        optionListHeading = "%n@|bold,underline OPTIONS|@%n%n",
        parameterListHeading = "%n@|bold,underline PARAMETERS|@%n%n",
        showDefaultValues = true)
public abstract class AbstractCommand implements Callable<Integer> {
    @Option(names = "--verbose", arity = "0", description = "Enable verbose output.")
    protected boolean verbose;

    @Option(names = "--expand-commands", description = "Allow command expansion in config value evaluation.")
    protected boolean allowCommandExpansion;

    protected final ExecutionContext ctx;
    // Admin commands are generally forked, because it is the only way how their JVM can configured.
    // Some admin commands are used to just launch the DBMS instead of executing an administration task.
    // As an optimisation, such commands should not fork, because we might end up with
    // three running JVMs if they do - boostrap JVM, forked command JVM and DBMS JVM.
    private final boolean fork;

    @Spec
    protected CommandSpec spec;

    protected AbstractCommand(ExecutionContext ctx, boolean fork) {
        this.ctx = requireNonNull(ctx);
        this.fork = fork;
    }

    protected AbstractCommand(ExecutionContext ctx) {
        this(ctx, true);
    }

    protected abstract void execute() throws Exception;

    @Override
    public Integer call() throws Exception {
        if (verbose) {
            printVerboseHeader();
        }
        try {
            execute();
        } catch (CommandFailedException e) {
            if (verbose) {
                e.printStackTrace(ctx.err());
            } else {
                ctx.err().println(e.getMessage());
                ctx.err().println("Run with '--verbose' for a more detailed error message.");
            }
            return e.getExitCode();
        }
        return 0;
    }

    public boolean shouldFork() {
        return fork;
    }

    private void printVerboseHeader() {
        PrintStream out = ctx.out();
        out.println("neo4j " + Version.getNeo4jVersion());
        SystemDiagnostics.JAVA_VIRTUAL_MACHINE.dump(out::println);
    }
}
