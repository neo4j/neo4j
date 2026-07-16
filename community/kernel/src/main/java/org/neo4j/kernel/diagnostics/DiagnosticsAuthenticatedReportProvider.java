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
package org.neo4j.kernel.diagnostics;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;

/**
 * Base class for a provider of authenticated reports. In contrast to {@link DiagnosticsOfflineReportProvider}, an authenticated
 * provider requires a running instance of the database: it is handed an authenticated {@link DiagnosticsLiveConnection}
 * and gathers information by running procedures/queries against the live DBMS.
 * <p>
 * All implementing classes are service loaded (annotate with {@code @ServiceProvider}) and initialized through
 * {@link #init(FileSystemAbstraction, Config, Set)}. When at least one of an authenticated provider's classifiers is
 * requested, the diagnostics command opens a single shared connection and invokes
 * {@link #provideSources(Set, DiagnosticsLiveConnection)} on every authenticated provider before closing it again.
 * Implementations are therefore expected to run their queries <em>eagerly</em> within that call and return sources
 * backed by the captured results (e.g. via {@link DiagnosticsReportSources#newDiagnosticsString}).
 */
@Service
public abstract class DiagnosticsAuthenticatedReportProvider {
    private final Set<String> filterClassifiers;

    /**
     * A provider needs to know all the available classifiers in advance. A classifier is a group in the context of
     * diagnostics reporting, e.g. 'logs', 'config' or 'threaddump'.
     *
     * @param classifier the primary classifier this provider handles.
     * @param classifiers any additional classifiers this provider handles.
     */
    protected DiagnosticsAuthenticatedReportProvider(String classifier, String... classifiers) {
        filterClassifiers = new HashSet<>(Arrays.asList(classifiers));
        filterClassifiers.add(classifier);
    }

    /**
     * Called after service loading to initialize the class.
     * @param fs filesystem to use for file access.
     * @param config configuration file in use.
     * @param databaseNames the databases to report for.
     */
    public abstract void init(FileSystemAbstraction fs, Config config, Set<String> databaseNames);

    /**
     * Runs the relevant checks against the live DBMS and returns sources to include in the report, grouped by the
     * classifier they belong to. Implementations should execute their queries eagerly while {@code connection} is open.
     *
     * @param classifiers the set of classifiers to filter on.
     * @param connection an authenticated, open connection to the running DBMS.
     * @return sources keyed by classifier, empty if nothing matches.
     */
    protected abstract Map<String, List<DiagnosticsReportSource>> provideSources(
            Set<String> classifiers, DiagnosticsLiveConnection connection);

    /**
     * Human-readable description of a classifier, shown by {@code --list}. Defaults to a generic message; override to
     * provide something more specific.
     *
     * @param classifier one of this provider's classifiers.
     * @return a one-line description.
     */
    protected String describe(String classifier) {
        return "include " + classifier + " collected from the running instance";
    }

    final Set<String> getFilterClassifiers() {
        return filterClassifiers;
    }

    final Map<String, List<DiagnosticsReportSource>> getDiagnosticsSources(
            Set<String> classifiers, DiagnosticsLiveConnection connection) {
        if (classifiers.contains("all")) {
            return provideSources(filterClassifiers, connection);
        } else {
            return provideSources(classifiers, connection);
        }
    }

    final String describeClassifier(String classifier) {
        return describe(classifier);
    }
}
