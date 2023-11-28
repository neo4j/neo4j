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

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.shell.TransactionHandler.TransactionType.USER_ACTION;
import static org.neo4j.shell.util.Versions.version;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.prettyprint.TableOutputFormatter;
import org.neo4j.shell.printer.Printer;
import org.neo4j.shell.util.Version;

/**
 * Print neo4j system information
 */
public class SysInfo implements Command {
    private final Printer printer;
    private final TableOutputFormatter tableFormatter;
    private final CypherShell shell;
    private final Version firstSupportedVersion = version("4.4.0");
    private final String SYSTEM_DB_TYPE = "system";
    private final String COMPOSITE_DB_TYPE = "composite";

    public SysInfo(Printer printer, CypherShell shell) {
        this.printer = printer;
        this.tableFormatter = new TableOutputFormatter(false, 100);
        this.shell = shell;
    }

    @Override
    public void execute(List<String> args) throws ExitException, CommandException {
        requireArgumentCount(args, 0);

        final var version = shell.getServerVersion();
        if (!shell.isConnected()) {
            throw new CommandException("Connect to a database to use :sysinfo");
        } else if (version != null
                && !version.isBlank()
                && version(shell.getServerVersion()).compareTo(firstSupportedVersion) < 0) {
            throw new CommandException(":sysinfo is only supported since " + firstSupportedVersion);
        } else if (isSystemOrCompositeDb()) {
            throw new CommandException(
                    "The :sysinfo command is not supported while using the system or a composite database.");
        } else {
            final var clientConfig = clientConfig();
            final var db = shell.getActualDatabaseAsReportedByServer();
            printDatabases();

            for (final var group : allMetrics) {
                printMetrics(clientConfig, db, group);
            }
        }
    }

    private boolean isSystemOrCompositeDb() throws CommandException {
        final var dbName = shell.getActualDatabaseAsReportedByServer();
        final var query = "SHOW DATABASES WHERE name = $db";

        final var result = shell.runCypher(query, Map.of("db", Values.value(dbName)), USER_ACTION);
        if (result.isPresent()) {
            for (final var record : result.get().getRecords()) {
                final var dbType = record.get("type").asString("");
                if (SYSTEM_DB_TYPE.equals(dbType)
                        || COMPOSITE_DB_TYPE.equals(dbType)
                        || (dbType.isEmpty() && "system".equals(dbName))) {
                    return true;
                }
            }
        }
        return false;
    }

    private ClientConfig clientConfig() throws CommandException {
        final var clientConfigMap = shell.runCypher("CALL dbms.clientConfig() yield name, value", Map.of(), USER_ACTION)
                .map(result -> result.getRecords().stream()
                        .collect(toMap(r -> r.get("name").asString(), r -> r.get("value")
                                .asString())))
                .orElseGet(Map::of);
        final var serverMetricsPrefix = ofNullable(clientConfigMap.get("server.metrics.prefix")) // Version 5
                .or(() -> ofNullable(clientConfigMap.get("metrics.prefix"))) // Version 4
                .orElse("neo4j");

        // Only 4
        final var namespacesEnabled =
                ofNullable(clientConfigMap.get("metrics.namespaces.enabled")).map("true"::equals);

        return new ClientConfig(serverMetricsPrefix, namespacesEnabled);
    }

    private void printDatabases() throws CommandException {
        final var query =
                """
                SHOW DATABASES YIELD
                  name AS Name,
                  address AS Address,
                  role AS Role,
                  currentStatus AS Status,
                  default AS Default""";
        shell.runCypher(query, Map.of(), USER_ACTION).ifPresent(result -> {
            printer.printOut("");
            tableFormatter.formatWithHeading(result, printer, "Databases");
        });
    }

    private void printMetrics(ClientConfig config, String database, MetricGroup group) throws CommandException {
        final var query =
                """
                UNWIND $metrics as metric
                CALL dbms.queryJmx(metric.name) YIELD name, attributes
                WITH metric.displayName AS Name, attributes.Value.value AS value, attributes.Count.value AS count
                RETURN
                  Name,
                  CASE WHEN value IS NOT NULL then value ELSE count END AS Value""";
        final var metricNamesParam = group.metrics().stream()
                .map(m -> Map.of("name", m.fullName(config, database), "displayName", m.displayName()))
                .toList();
        final var params = Map.of("metrics", Values.value(metricNamesParam));
        shell.runCypher(query, params, USER_ACTION).ifPresent(result -> {
            printer.printOut("");
            tableFormatter.formatWithHeading(result, printer, group.name());
        });
    }

    final List<MetricGroup> allMetrics = List.of(
            new MetricGroup(
                    "ID Allocation",
                    List.of(
                            Metric.db("ids_in_use.property", "Property ID"),
                            Metric.db("ids_in_use.relationship", "Relationship ID"),
                            Metric.db("ids_in_use.relationship_type", "Relationship Type ID"))),
            new MetricGroup(
                    "Store Size",
                    List.of(Metric.db("store.size.total", "Total"), Metric.db("store.size.database", "Database"))),
            new MetricGroup(
                    "Page Cache",
                    List.of(
                            Metric.dbms("page_cache.hits", "Hits"),
                            Metric.dbms("page_cache.hit_ratio", "Hit Ratio"),
                            Metric.dbms("page_cache.usage_ratio", "Usage Ratio"),
                            Metric.dbms("page_cache.page_faults", "Page Faults"))),
            new MetricGroup(
                    "Transactions",
                    List.of(
                            Metric.db("transaction.last_committed_tx_id", "Last Tx Id"),
                            Metric.db("transaction.active_read", "Current Read"),
                            Metric.db("transaction.active_write", "Current Write"),
                            Metric.db("transaction.peak_concurrent", "Peak Transactions"),
                            Metric.db("transaction.committed_read", "Committed Read"),
                            Metric.db("transaction.committed_write", "Committed Write"))));

    public static class Factory implements Command.Factory {
        @Override
        public Metadata metadata() {
            var help = "':sysinfo' prints neo4j system information";
            return new Metadata(":sysinfo", "Neo4j system information", "", help, List.of());
        }

        @Override
        public Command executor(Arguments args) {
            return new SysInfo(args.printer(), args.cypherShell());
        }
    }
}

record Metric(String metricName, String displayName, Function<Value, Value> mapper, boolean dbType) {

    String fullName(ClientConfig config, String database) {
        // It's a pain to query jmx across versions 🤯
        // https://neo4j.com/docs/operations-manual/current/monitoring/metrics/reference/
        // https://neo4j.com/docs/operations-manual/4.4/monitoring/metrics/reference/
        final var builder =
                new StringBuilder(config.metricsDomain()).append(":name=").append(config.serverMetricsPrefix());
        final var namespacesEnables = config.namespacesEnabled().orElse(true);
        if (dbType) {
            if (namespacesEnables) builder.append(".database");
            builder.append(".").append(database);
        } else if (namespacesEnables) {
            builder.append(".dbms");
        }
        return builder.append(".").append(metricName).toString();
    }

    static Metric db(String metricName, String displayName) {
        return new Metric(metricName, displayName, Function.identity(), true);
    }

    static Metric dbms(String metricName, String displayName) {
        return new Metric(metricName, displayName, Function.identity(), false);
    }
}

record MetricGroup(String name, List<Metric> metrics) {}

record ClientConfig(String serverMetricsPrefix, Optional<Boolean> namespacesEnabled) {
    String metricsDomain() {
        return serverMetricsPrefix + ".metrics";
    }
}
