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
package org.neo4j.shell;

import static java.lang.String.format;
import static org.neo4j.shell.ShellRunner.shouldBeInteractive;
import static org.neo4j.shell.terminal.CypherShellTerminalBuilder.terminalBuilder;
import static org.neo4j.shell.util.Versions.isPasswordChangeRequiredException;

import java.io.PrintStream;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.shell.build.Build;
import org.neo4j.shell.cli.CliArgHelper;
import org.neo4j.shell.cli.CliArgs;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parser.ShellStatementParser;
import org.neo4j.shell.parser.StatementParser;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.printer.AnsiPrinter;
import org.neo4j.shell.printer.Printer;
import org.neo4j.shell.state.BoltStateHandler;
import org.neo4j.shell.terminal.CypherShellTerminal;
import org.neo4j.util.VisibleForTesting;

public class Main implements AutoCloseable {
    private static final Logger log = Logger.create();
    public static final int EXIT_FAILURE = 1;
    public static final int EXIT_SUCCESS = 0;
    static final String NEO_CLIENT_ERROR_SECURITY_UNAUTHORIZED = "Neo.ClientError.Security.Unauthorized";
    private final CliArgs args;
    private final Printer printer;
    private final CypherShell shell;
    private final boolean isOutputInteractive;
    private final ShellRunner.Factory runnerFactory;
    private final CypherShellTerminal terminal;
    private final StatementParser statementParser = new ShellStatementParser();
    private final ParameterService parameters;

    public Main(CliArgs args) {
        boolean isInteractive = !args.getNonInteractive() && ShellRunner.isInputInteractive();
        this.printer = new AnsiPrinter(Format.VERBOSE, System.out, System.err);
        this.args = args;
        var boltStateHandler = new BoltStateHandler(shouldBeInteractive(args, isInteractive), args.getAccessMode());
        this.parameters = ParameterService.create(boltStateHandler);
        this.terminal = terminalBuilder()
                .interactive(isInteractive)
                .logger(printer)
                .parameters(parameters)
                .idleTimeout(args.getIdleTimeout(), args.getIdleTimeoutDelay())
                .build();
        this.shell = new CypherShell(
                printer, boltStateHandler, new PrettyPrinter(PrettyConfig.from(args, isInteractive)), parameters);
        this.isOutputInteractive = !args.getNonInteractive() && ShellRunner.isOutputInteractive();
        this.runnerFactory = new ShellRunner.Factory();
    }

    @VisibleForTesting
    public Main(
            CliArgs args, PrintStream out, PrintStream err, boolean outputInteractive, CypherShellTerminal terminal) {
        this.terminal = terminal;
        this.args = args;
        this.printer = new AnsiPrinter(Format.VERBOSE, out, err);
        final var isInteractive = shouldBeInteractive(args, terminal.isInteractive());
        var boltStateHandler = new BoltStateHandler(isInteractive, args.getAccessMode());
        this.parameters = ParameterService.create(boltStateHandler);
        this.shell = new CypherShell(
                printer, boltStateHandler, new PrettyPrinter(PrettyConfig.from(args, isInteractive)), parameters);
        this.isOutputInteractive = outputInteractive;
        this.runnerFactory = new ShellRunner.Factory();
    }

    @VisibleForTesting
    public Main(
            CliArgs args,
            AnsiPrinter logger,
            CypherShell shell,
            ParameterService parameters,
            boolean outputInteractive,
            ShellRunner.Factory runnerFactory,
            CypherShellTerminal terminal) {
        this.terminal = terminal;
        this.args = args;
        this.printer = logger;
        this.shell = shell;
        this.isOutputInteractive = outputInteractive;
        this.runnerFactory = runnerFactory;
        this.parameters = parameters;
    }

    public static void main(String[] args) {
        CliArgs cliArgs = new CliArgHelper(new Environment()).parse(args);

        // if null, then command line parsing went wrong
        // CliArgs has already printed errors.
        if (cliArgs == null) {
            System.exit(1);
        }

        Logger.setupLogging(cliArgs);

        int exitCode;
        try (var main = new Main(cliArgs)) {
            exitCode = main.startShell();
        }
        System.exit(exitCode);
    }

    public int startShell() {
        if (args.getVersion()) {
            terminal.write().println("Cypher-Shell " + Build.version());
            return EXIT_SUCCESS;
        }
        if (args.getDriverVersion()) {
            terminal.write().println("Neo4j Driver " + Build.driverVersion());
            return EXIT_SUCCESS;
        }
        if (args.getChangePassword()) {
            return runSetNewPassword();
        }

        return runShell();
    }

    private int runSetNewPassword() {
        try {
            promptAndChangePassword(args.connectionConfig(), null);
        } catch (Exception e) {
            log.error(e);
            printer.printError("Failed to change password: " + e.getMessage());
            return EXIT_FAILURE;
        }
        return EXIT_SUCCESS;
    }

    private int runShell() {
        ConnectionConfig connectionConfig = args.connectionConfig();
        try {
            // If user is passing in a cypher statement just run that and be done with it
            if (args.getCypher().isPresent()) {
                // Can only prompt for password if input has not been redirected
                connectMaybeInteractively(connectionConfig);
                shell.execute(statementParser.parse(args.getCypher().get()).statements());
                return EXIT_SUCCESS;
            } else {
                // Can only prompt for password if input has not been redirected
                connectMaybeInteractively(connectionConfig);

                // Construct shellrunner after connecting, due to interrupt handling
                ShellRunner shellRunner = runnerFactory.create(args, shell, printer, terminal);
                CommandHelper commandHelper =
                        new CommandHelper(printer, shellRunner.getHistorian(), shell, terminal, parameters);

                shell.setCommandHelper(commandHelper);

                // Print some messages if needed
                shell.printFallbackWarning(connectionConfig.uri());
                if (shellRunner.isInteractive() || args.getFormat() == Format.VERBOSE) {
                    // Restrict this message to not break machine readability in some cases
                    shell.printLicenseWarnings();
                }

                return shellRunner.runUntilEnd();
            }
        } catch (Throwable e) {
            log.error(e);
            printer.printError(e);
            return EXIT_FAILURE;
        }
    }

    /**
     * Connect the shell to the server, and try to handle missing passwords and such.
     */
    private void connectMaybeInteractively(ConnectionConfig connectionConfig) throws Exception {
        boolean didPrompt = false;

        // Prompt directly in interactive mode if user provided username but not password
        if (terminal.isInteractive()
                && !connectionConfig.username().isEmpty()
                && connectionConfig.password().isEmpty()) {
            connectionConfig = promptForUsernameAndPassword(connectionConfig);
            didPrompt = true;
        }

        while (true) {
            try {
                // Try to connect
                shell.connect(connectionConfig);
                setArgumentParameters();
                return;
            } catch (AuthenticationException e) {
                // Fail if we already prompted,
                // or do not have interactive input,
                // or already tried with both username and password
                if (didPrompt
                        || !terminal.isInteractive()
                        || !connectionConfig.username().isEmpty()
                                && !connectionConfig.password().isEmpty()) {
                    log.error("Failed to connect", e);
                    throw e;
                }

                // Otherwise we prompt for username and password, and try to connect again
                log.info("Failed to connect, prompting for user name and password...");
                connectionConfig = promptForUsernameAndPassword(connectionConfig);
                didPrompt = true;
            } catch (Neo4jException e) {
                if (terminal.isInteractive() && isPasswordChangeRequiredException(e)) {
                    connectionConfig = promptAndChangePassword(connectionConfig, "Password change required");
                    didPrompt = true;
                } else {
                    log.error("Failed to connect", e);
                    throw e;
                }
            }
        }
    }

    private void setArgumentParameters() throws CommandException {
        for (var parameter : args.getParameters()) {
            parameters.setParameters(parameters.evaluate(parameter));
        }
    }

    private ConnectionConfig promptForUsernameAndPassword(ConnectionConfig connectionConfig) throws Exception {
        String username = connectionConfig.username();
        String password = connectionConfig.password();
        if (username.isEmpty()) {
            username =
                    isOutputInteractive ? promptForNonEmptyText("username", false) : promptForText("username", false);
        }
        if (password.isEmpty()) {
            password = promptForText("password", true);
        }
        return connectionConfig.withUsernameAndPassword(username, password);
    }

    private ConnectionConfig promptAndChangePassword(ConnectionConfig connectionConfig, String message)
            throws Exception {
        log.info("Password change triggered.");
        if (message != null) {
            terminal.write().println(message);
        }
        String username = connectionConfig.username();
        if (username.isEmpty()) {
            username =
                    isOutputInteractive ? promptForNonEmptyText("username", false) : promptForText("username", false);
        }
        String password = connectionConfig.password();
        if (password.isEmpty()) {
            password = promptForText("password", true);
        }
        connectionConfig = connectionConfig.withUsernameAndPassword(username, password);
        String newPassword =
                isOutputInteractive ? promptForNonEmptyText("new password", true) : promptForText("new password", true);
        String reenteredNewPassword = promptForText("confirm password", true);

        if (!reenteredNewPassword.equals(newPassword)) {
            throw new CommandException("Passwords are not matching.");
        }

        shell.changePassword(connectionConfig, newPassword);
        return connectionConfig.withPassword(newPassword);
    }

    @VisibleForTesting
    protected CypherShell getCypherShell() {
        return shell;
    }

    private String promptForNonEmptyText(String prompt, boolean maskInput) throws Exception {
        String text = promptForText(prompt, maskInput);
        while (text.isEmpty()) {
            text = promptForText(format("%s cannot be empty%n%n%s", prompt, prompt), maskInput);
        }
        return text;
    }

    private String promptForText(String prompt, boolean maskInput) throws CommandException {
        String read;
        try (final var simplePrompt = terminal.simplePrompt()) {
            final var promptWithColon = prompt + ": ";
            read = maskInput ? simplePrompt.readPassword(promptWithColon) : simplePrompt.readLine(promptWithColon);
        } catch (Exception e) {
            log.error(e);
            throw new CommandException("No text could be read, exiting...");
        }
        if (read == null) {
            throw new CommandException("No text could be read, exiting...");
        }
        return read;
    }

    @Override
    public void close() {
        try {
            terminal.close();
            shell.disconnect();
        } catch (Exception e) {
            log.warn("Failed to exit", e);
        }
    }
}
