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

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;
import static org.neo4j.shell.cli.CliArgs.DEFAULT_HOST;
import static org.neo4j.shell.cli.CliArgs.DEFAULT_PORT;
import static org.neo4j.shell.cli.CliArgs.DEFAULT_SCHEME;
import static org.neo4j.shell.cli.FailBehavior.FAIL_AT_END;
import static org.neo4j.shell.cli.FailBehavior.FAIL_FAST;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.impl.action.StoreConstArgumentAction;
import net.sourceforge.argparse4j.impl.action.StoreTrueArgumentAction;
import net.sourceforge.argparse4j.impl.choice.CollectionArgumentChoice;
import net.sourceforge.argparse4j.impl.type.BooleanArgumentType;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;
import net.sourceforge.argparse4j.inf.FeatureControl;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import org.neo4j.shell.Environment;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.terminal.CypherShellTerminal;

/**
 * Command line argument parsing and related stuff
 */
public class CliArgHelper {
    private static final Logger log = Logger.create();
    public static final String USERNAME_ENV_VAR = "NEO4J_USERNAME";
    public static final String PASSWORD_ENV_VAR = "NEO4J_PASSWORD";
    public static final String DATABASE_ENV_VAR = "NEO4J_DATABASE";
    public static final String ADDRESS_ENV_VAR = "NEO4J_ADDRESS";
    public static final String URI_ENV_VAR = "NEO4J_URI";
    private static final String HISTORY_ENV_VAR = "NEO4J_CYPHER_SHELL_HISTORY";
    private static final String DEFAULT_ADDRESS = format("%s://%s:%d", DEFAULT_SCHEME, DEFAULT_HOST, DEFAULT_PORT);
    private static final AccessMode DEFAULT_ACCESS_MODE = AccessMode.WRITE;
    public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofSeconds(-1);
    public static final Duration DEFAULT_IDLE_TIMEOUT_DELAY = Duration.ofMinutes(5);

    private final Environment environment;

    public CliArgHelper(Environment environment) {
        this.environment = environment;
    }

    /**
     * @param args to parse
     * @return null in case of error, commandline arguments otherwise
     */
    public CliArgs parse(String... args) {
        try {
            return parseAndThrow(args);
        } catch (ArgumentParserException e) {
            e.getParser().handleError(e);
            return null;
        }
    }

    private static void preValidateArguments(ArgumentParser parser, String... args) throws ArgumentParserException {
        if (Arrays.asList(args).contains("-file")) {
            throw new ArgumentParserException("Unrecognized argument '-file', did you mean --file?", parser);
        }
    }

    /**
     * Parse command line arguments, including environmental variable fallbacks.
     *
     * @param args to parse
     * @return commandline arguments
     * @throws ArgumentParserException if an argument can't be parsed.
     */
    public CliArgs parseAndThrow(String... args) throws ArgumentParserException {
        final CliArgs cliArgs = new CliArgs();
        final ArgumentParser parser = setupParser();
        preValidateArguments(parser, args);
        return getCliArgs(cliArgs, parser, parser.parseArgs(args));
    }

    private Optional<String> addressFromEnvironment() {
        final var address = ofNullable(environment.getVariable(ADDRESS_ENV_VAR));
        final var uri = ofNullable(environment.getVariable(URI_ENV_VAR));

        if (address.isPresent() && uri.isPresent()) {
            throw new IllegalArgumentException(
                    "Specify one or none of environment variables " + ADDRESS_ENV_VAR + " and " + URI_ENV_VAR);
        }

        return address.or(() -> uri);
    }

    private CliArgs getCliArgs(CliArgs cliArgs, ArgumentParser parser, Namespace ns) throws ArgumentParserException {
        final var address = ofNullable(ns.getString("address"))
                .or(this::addressFromEnvironment)
                .orElse(DEFAULT_ADDRESS);
        final URI uri = parseURI(parser, address);

        // ---------------------
        // Connection arguments
        cliArgs.setUri(uri);

        // Also parse username and password from address if available
        parseUserInfo(uri, cliArgs);

        // Only overwrite user from address string if the argument were specified
        ofNullable(ns.getString("username"))
                .or(() -> ofNullable(environment.getVariable(USERNAME_ENV_VAR)))
                .ifPresent(user -> cliArgs.setUsername(user, cliArgs.getUsername()));

        // Only overwrite password from address string if the argument were specified
        ofNullable(ns.getString("password"))
                .or(() -> ofNullable(environment.getVariable(PASSWORD_ENV_VAR)))
                .ifPresent(pass -> cliArgs.setPassword(pass, cliArgs.getPassword()));

        String impersonatedUser = ns.getString("impersonate");
        if (impersonatedUser != null) {
            cliArgs.setImpersonatedUser(impersonatedUser);
        }

        cliArgs.setAccessMode(ns.get("access-mode"));

        cliArgs.setEncryption(Encryption.parse(ns.get("encryption")));

        final var database = ofNullable(ns.getString("database"))
                .or(() -> ofNullable(environment.getVariable(DATABASE_ENV_VAR)))
                .orElse(ABSENT_DB_NAME);
        cliArgs.setDatabase(database);

        cliArgs.setInputFilename(ns.getString("file"));

        // ----------------
        // Other arguments
        // cypher string might not be given, represented by null
        cliArgs.setCypher(ns.getString("cypher"));
        // Fail behavior as sensible default and returns a proper type
        cliArgs.setFailBehavior(ns.get("fail-behavior"));

        // Set Output format
        cliArgs.setFormat(Format.parse(ns.get("format")));

        cliArgs.setParameters(ns.getList("param"));

        cliArgs.setNonInteractive(ns.getBoolean("force-non-interactive"));

        cliArgs.setWrap(ns.getBoolean("wrap"));

        cliArgs.setNumSampleRows(ns.getInt("sample-rows"));

        cliArgs.setVersion(ns.getBoolean("version"));

        cliArgs.setDriverVersion(ns.getBoolean("driver-version"));

        cliArgs.setChangePassword(ns.getBoolean("change-password"));

        cliArgs.setLogHandler(ns.get("log-file"));

        final var historyBehaviour = ofNullable(ns.<CypherShellTerminal.HistoryBehaviour>get("history-behaviour"))
                .or(() -> ofNullable(environment.getVariable(HISTORY_ENV_VAR))
                        .map(HistoryBehaviourHandler::historyFromFilePath))
                .orElseGet(CypherShellTerminal.DefaultHistory::new);
        cliArgs.setHistoryBehaviour(historyBehaviour);

        cliArgs.setNotificationsEnabled(ns.getBoolean("enable-notifications"));

        ofNullable(ns.<Duration>get("idle-timeout")).ifPresent(cliArgs::setIdleTimeout);
        ofNullable(ns.<Duration>get("hidden-idle-timeout-delay")).ifPresent(cliArgs::setIdleTimeoutDelay);

        return cliArgs;
    }

    private static void parseUserInfo(URI uri, CliArgs cliArgs) {
        String userInfo = uri.getUserInfo();
        String user = null;
        String password = null;
        if (userInfo != null) {
            String[] split = userInfo.split(":");
            if (split.length == 0) {
                user = userInfo;
            } else if (split.length == 2) {
                user = split[0];
                password = split[1];
            } else {
                throw new IllegalArgumentException("Cannot parse user and password from " + userInfo);
            }
        }
        cliArgs.setUsername(user, "");
        cliArgs.setPassword(password, "");
    }

    static URI parseURI(ArgumentParser parser, String address) throws ArgumentParserException {
        try {
            if (!address.contains("://")) {
                // URI can't parse addresses without scheme, prepend fake "bolt://" to reuse the parsing facility
                address = DEFAULT_SCHEME + "://" + address;
            }

            var uri = new URI(address);
            if (uri.getPort() == -1) {
                uri = new URI(
                        uri.getScheme(),
                        uri.getUserInfo(),
                        uri.getHost(),
                        DEFAULT_PORT,
                        uri.getPath(),
                        uri.getQuery(),
                        uri.getFragment());
            }
            return uri;
        } catch (URISyntaxException e) {
            log.error(e);
            var message =
                    """
                    cypher-shell: error: Failed to parse address: '%s'
                    Address should be of the form: [scheme://][username:password@][host][:port]"""
                            .formatted(address);
            throw new ArgumentParserException(message, e, parser);
        }
    }

    private static ArgumentParser setupParser() {
        ArgumentParser parser = ArgumentParsers.newFor("cypher-shell")
                .defaultFormatWidth(100)
                .build()
                .defaultHelp(true)
                .description(format(
                        "Cypher Shell is a command-line tool used to run queries and perform administrative tasks "
                                + "against a Neo4j instance. By default, the shell is interactive, but you can also use it for scripting "
                                + "by passing Cypher directly on the command line or by piping a file with Cypher statements "
                                + "(requires Powershell on Windows). "
                                + "It communicates via the Bolt protocol."
                                + "%n%n"
                                + "Example of piping a file:%n"
                                + "  cat some-cypher.txt | cypher-shell"));

        ArgumentGroup connGroup = parser.addArgumentGroup("connection arguments");
        connGroup
                .addArgument("-a", "--address", "--uri")
                .action(new OnceArgumentAction())
                .help("Address and port to connect to. Defaults to " + DEFAULT_ADDRESS
                        + ". Can also be specified using the environment variable " + ADDRESS_ENV_VAR + " or "
                        + URI_ENV_VAR
                        + ".");
        connGroup
                .addArgument("-u", "--username")
                .help("Username to connect as. Can also be specified using the environment variable " + USERNAME_ENV_VAR
                        + ".");
        connGroup.addArgument("--impersonate").help("User to impersonate.");
        connGroup
                .addArgument("-p", "--password")
                .help("Password to connect with. Can also be specified using the environment variable "
                        + PASSWORD_ENV_VAR + ".");
        connGroup
                .addArgument("--encryption")
                .help("Whether the connection to Neo4j should be encrypted. This must be consistent with the Neo4j's "
                        + "configuration. If choosing '"
                        + Encryption.DEFAULT.name().toLowerCase(Locale.ROOT)
                        + "', the encryption setting is deduced from the specified address. "
                        + "For example, the 'neo4j+ssc' protocol uses encryption.")
                .choices(new CollectionArgumentChoice<>(
                        Encryption.TRUE.name().toLowerCase(Locale.ROOT),
                        Encryption.FALSE.name().toLowerCase(Locale.ROOT),
                        Encryption.DEFAULT.name().toLowerCase(Locale.ROOT)))
                .setDefault(Encryption.DEFAULT.name().toLowerCase(Locale.ROOT));
        connGroup
                .addArgument("-d", "--database")
                .help("Database to connect to. Can also be specified using the environment variable " + DATABASE_ENV_VAR
                        + ".");
        connGroup
                .addArgument("--access-mode")
                .dest("access-mode")
                .type(Arguments.caseInsensitiveEnumStringType(AccessMode.class))
                .setDefault(DEFAULT_ACCESS_MODE)
                .help("Access mode. Defaults to " + DEFAULT_ACCESS_MODE.name() + ".");
        MutuallyExclusiveGroup failGroup = parser.addMutuallyExclusiveGroup();
        failGroup
                .addArgument("--fail-fast")
                .help(
                        "Exit and report failure on the first error when reading from a file (this is the default behavior).")
                .dest("fail-behavior")
                .setConst(FAIL_FAST)
                .action(new StoreConstArgumentAction());
        failGroup
                .addArgument("--fail-at-end")
                .help("Exit and report failures at the end of the input when reading from a file.")
                .dest("fail-behavior")
                .setConst(FAIL_AT_END)
                .action(new StoreConstArgumentAction());
        parser.setDefault("fail-behavior", FAIL_FAST);

        parser.addArgument("--format")
                .help(
                        """
                                Desired output format. Displays the results in tabular format if you use the shell interactively \
                                and with minimal formatting if you use it for scripting.
                                `verbose` displays results in tabular format and prints statistics.
                                `plain` displays data with minimal formatting.""")
                .choices(new CollectionArgumentChoice<>(
                        Format.AUTO.name().toLowerCase(Locale.ROOT),
                        Format.VERBOSE.name().toLowerCase(Locale.ROOT),
                        Format.PLAIN.name().toLowerCase(Locale.ROOT)))
                .setDefault(Format.AUTO.name().toLowerCase(Locale.ROOT));

        parser.addArgument("-P", "--param")
                .help("Add a parameter to this session."
                        + " Example: `-P {a: 1}` or `-P {a: 1, b: duration({seconds: 1})}`."
                        + " This argument can be specified multiple times.")
                .action(new AddParamArgumentAction(ParameterService.createParser()))
                .setDefault(new ArrayList<ParameterService.RawParameters>());

        parser.addArgument("--non-interactive")
                .help("Force non-interactive mode. Only useful when auto-detection fails (like on Windows).")
                .dest("force-non-interactive")
                .action(new StoreTrueArgumentAction());

        parser.addArgument("--sample-rows")
                .help("Number of rows sampled to compute table widths (only for format=VERBOSE).")
                .type(new PositiveIntegerType())
                .dest("sample-rows")
                .setDefault(CliArgs.DEFAULT_NUM_SAMPLE_ROWS);

        parser.addArgument("--wrap")
                .help("Wrap table column values if column is too narrow (only for format=VERBOSE).")
                .type(new BooleanArgumentType())
                .setDefault(true);

        parser.addArgument("-v", "--version")
                .help("Print Cypher Shell version and exit.")
                .action(new StoreTrueArgumentAction());

        parser.addArgument("--driver-version")
                .help("Print Neo4j Driver version and exit.")
                .dest("driver-version")
                .action(new StoreTrueArgumentAction());

        parser.addArgument("cypher").nargs("?").help("An optional string of Cypher to execute and then exit.");
        parser.addArgument("-f", "--file")
                .help(
                        "Pass a file with Cypher statements to be executed. After executing all statements, Cypher Shell shuts down.");

        parser.addArgument("--change-password")
                .action(Arguments.storeTrue())
                .dest("change-password")
                .help("Change the neo4j user password and exit.");

        parser.addArgument("--log")
                .nargs("?")
                .type(new LogHandlerType())
                .dest("log-file")
                .help("Enable logging to the specified file, or standard error if the file is omitted.")
                .setDefault((LogHandlerType) null)
                .setConst(new ConsoleHandler());

        parser.addArgument("--history")
                .help(
                        "File path of a query and a command history file or `in-memory` for in-memory history. Defaults to <user home>/.neo4j/.cypher_shell_history. Can also be set using the environment variable "
                                + HISTORY_ENV_VAR + ".")
                .dest("history-behaviour")
                .type(new HistoryBehaviourHandler())
                .setDefault((CypherShellTerminal.HistoryBehaviour) null);

        parser.addArgument("--notifications")
                .help("Enable notifications in interactive mode.")
                .dest("enable-notifications")
                .action(Arguments.storeTrue());

        parser.addArgument("--idle-timeout")
                .dest("idle-timeout")
                .type(new TimeoutHandler())
                .help(
                        "Closes the application after the specified amount of idle time in interactive mode. You can specify the duration using the format `<hours>h<minutes>m<seconds>s`, for example `1h` (1 hour), `1h30m` (1 hour 30 minutes), or `30m` (30 minutes).");

        parser.addArgument("--hidden-idle-timeout-delay")
                .dest("hidden-idle-timeout-delay")
                .type(new TimeoutHandler())
                .help(FeatureControl.SUPPRESS);

        return parser;
    }

    private static class PositiveIntegerType implements ArgumentType<Integer> {
        @Override
        public Integer convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
            try {
                int result = Integer.parseInt(value);
                if (result < 1) {
                    throw new NumberFormatException(value);
                }
                return result;
            } catch (NumberFormatException nfe) {
                throw new ArgumentParserException("Invalid value: " + value, parser);
            }
        }
    }

    private static class LogHandlerType implements ArgumentType<Handler> {
        private static final int MAX_BYTES = 100_000_000;
        private static final int LOG_FILE_COUNT = 1;

        @Override
        public Handler convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
            try {
                return new FileHandler(value, MAX_BYTES, LOG_FILE_COUNT, false);
            } catch (IOException e) {
                throw new ArgumentParserException("Failed to open log file: " + e.getMessage(), parser);
            }
        }
    }

    private static class HistoryBehaviourHandler implements ArgumentType<CypherShellTerminal.HistoryBehaviour> {

        @Override
        public CypherShellTerminal.HistoryBehaviour convert(
                ArgumentParser argumentParser, Argument argument, String value) {
            if ("in-memory".equals(value.toLowerCase(Locale.ROOT))) {
                return new CypherShellTerminal.InMemoryHistory();
            } else {
                return historyFromFilePath(value);
            }
        }

        private static CypherShellTerminal.FileHistory historyFromFilePath(String path) {
            try {
                return new CypherShellTerminal.FileHistory(Path.of(path));
            } catch (InvalidPathException e) {
                throw new IllegalArgumentException("Invalid history file path " + path);
            }
        }
    }

    private static class TimeoutHandler implements ArgumentType<Duration> {

        @Override
        public Duration convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
            try {
                if ("disable".equalsIgnoreCase(value)) return null;
                else return Duration.parse("PT" + value);
            } catch (Exception e) {
                throw new ArgumentParserException(
                        "Invalid timeout value, valid values are for example 1h30m (1 hour and 30 minutes), 1h (1 hour), 10m (10 minutes): "
                                + value,
                        parser);
            }
        }
    }
}
