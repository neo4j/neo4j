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
package org.neo4j.procedure.impl;

import static java.time.ZoneOffset.UTC;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;

public class ProcedureConfig {
    private final List<Pattern> accessPatterns;
    private final List<Pattern> whiteList;
    private final ZoneId defaultTemporalTimeZone;
    private final List<String> reservedProcedureNamespaces;

    private ProcedureConfig() {
        this.accessPatterns = Collections.emptyList();
        this.whiteList = Collections.singletonList(compilePattern("*"));
        this.defaultTemporalTimeZone = UTC;
        this.reservedProcedureNamespaces = GraphDatabaseInternalSettings.reserved_procedure_namespaces.defaultValue();
    }

    public ProcedureConfig(Config config) {
        this.accessPatterns = parseMatchers(
                config.get(GraphDatabaseSettings.procedure_unrestricted), ProcedureConfig::compilePattern);
        this.whiteList =
                parseMatchers(config.get(GraphDatabaseSettings.procedure_allowlist), ProcedureConfig::compilePattern);
        this.defaultTemporalTimeZone = config.get(GraphDatabaseSettings.db_temporal_timezone);
        this.reservedProcedureNamespaces = config.get(GraphDatabaseInternalSettings.reserved_procedure_namespaces);
    }

    private <T> List<T> parseMatchers(List<String> fullAccessProcedures, Function<String, T> matchFunc) {
        if (fullAccessProcedures == null || fullAccessProcedures.isEmpty()) {
            return Collections.emptyList();
        }
        return fullAccessProcedures.stream().map(matchFunc).collect(Collectors.toList());
    }

    public boolean fullAccessFor(String procedureName) {
        return accessPatterns.stream()
                .anyMatch(pattern -> pattern.matcher(procedureName).matches());
    }

    public boolean isWhitelisted(String procedureName) {
        return whiteList.stream()
                .anyMatch(pattern -> pattern.matcher(procedureName).matches());
    }

    private static Pattern compilePattern(String procedure) {
        procedure = procedure.trim().replaceAll("([\\[\\]\\\\?()^${}+|.])", "\\\\$1");
        return Pattern.compile(procedure.replaceAll("\\*", ".*"));
    }

    public static final ProcedureConfig DEFAULT = new ProcedureConfig();

    public ZoneId getDefaultTemporalTimeZone() {
        return defaultTemporalTimeZone;
    }

    public List<String> reservedProcedureNamespaces() {
        return reservedProcedureNamespaces;
    }
}
