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
package org.neo4j.recovery;

import static java.lang.Boolean.getBoolean;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.ProcessUtils.getModuleOptions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class UniquenessRecoveryTest {
    /** This test can be configured (via system property) to use cypher or the core API to exercise the db. */
    private static final boolean USE_CYPHER = getBoolean(param("use_cypher"));
    /** This test can be configured (via system property) to run with all different kill signals. */
    public static final boolean EXHAUSTIVE = getBoolean(param("exhaustive"));

    /** these are all the kill signals that causes a JVM to exit */
    private static final int[] KILL_SIGNALS = {
        1 /*SIGHUP - should run exit hooks*/,
        2 /*SIGINT - should run exit hooks*/,
        /*skip 3 (SIGQUIT) - it only causes a thread dump*/
        // none of these permit exit hooks to run:
        4 /*SIGILL*/,
        5,
        6 /*SIGABRT*/,
        7,
        8 /*SIGFPE*/,
        9 /*SIGKILL*/,
        10 /*SIGBUS*/,
        11 /*SIGSEGV*/,
        12,
        14,
        // the "normal" kill signal:
        15 /*SIGTERM - should run exit hooks*/,
        // none of these permit exit hooks to run:
        24,
        26,
        27,
        30,
        31
    };

    private static DatabaseManagementService managementService;

    private static String param(String name) {
        return UniquenessRecoveryTest.class.getName() + "." + name;
    }

    @Inject
    public TestDirectory dir;

    private static final Field PID;

    static {
        Field pid;
        try {
            pid = Class.forName("java.lang.ProcessImpl").getDeclaredField("pid");
            pid.setAccessible(true);
        } catch (Throwable ex) {
            pid = null;
        }
        PID = pid;
    }

    /** This test uses sub-processes, the code in here is the orchestration of those processes. */
    @ParameterizedTest
    @MethodSource("configurations")
    public void shouldUpholdConstraintEvenAfterRestart(Configuration config) throws Exception {
        assumeTrue(PID != null); // this test can only run on UNIX

        // given
        Path path = dir.absolutePath();
        ArrayList<String> args = new ArrayList<>();
        args.addAll(List.of("java", "-ea", "-Xmx1G"));
        args.addAll(getModuleOptions());
        args.addAll(List.of(
                "-Dforce_create_constraint=" + config.force_create_constraint,
                "-D" + param("use_cypher") + "=" + USE_CYPHER,
                "-cp",
                System.getProperty("java.class.path"),
                getClass().getName(),
                path.toAbsolutePath().toString()));
        ProcessBuilder prototype = new ProcessBuilder(args);
        prototype.environment().put("JAVA_HOME", System.getProperty("java.home"));

        // when
        {
            Process process = prototype.start();
            if (awaitMessage(process, "kill me") != null) {
                throw new IllegalStateException("first process failed to execute properly");
            }
            kill(config.kill_signal, process);
            awaitMessage(process, null);
        }
        {
            Process process = prototype.start();
            Integer exitCode = awaitMessage(process, "kill me");
            if (exitCode == null) {
                kill(config.kill_signal, process);
                awaitMessage(process, null);
            } else if (exitCode != 0) {
            }
        }

        // then
        GraphDatabaseService db = graphdb(path);
        try {
            shouldHaveUniquenessConstraintForNamePropertyOnPersonLabel(db);
            nodesWithPersonLabelHaveUniqueName(db);
        } finally {
            managementService.shutdown();
        }
    }

    /** This is the code that the test actually executes to attempt to violate the constraint. */
    public static void main(String... args) throws Exception {
        Path path = Path.of(args[0]).toAbsolutePath();
        boolean createConstraint =
                getBoolean("force_create_constraint") || !Files.isRegularFile(path.resolve("neostore"));
        GraphDatabaseService db = graphdb(path);
        if (createConstraint) {
            try {
                createConstraint(db);
            } catch (Exception e) {
                if (e instanceof ConstraintViolationException) {
                } else {
                    System.exit(1);
                }
            }
        }
        try {
            addNode(db);
        } catch (ConstraintViolationException e) {
            managementService.shutdown();
            System.exit(0);
        } catch (Exception e) {
            System.exit(2);
        }

        flushPageCache(db);
        await();
    }

    // ASSERTIONS

    private static void shouldHaveUniquenessConstraintForNamePropertyOnPersonLabel(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            ConstraintDefinition constraint = Iterables.single(tx.schema().getConstraints());
            Assertions.assertEquals(ConstraintType.UNIQUENESS, constraint.getConstraintType());
            Assertions.assertEquals("Person", constraint.getLabel().name());
            Assertions.assertEquals("name", Iterables.single(constraint.getPropertyKeys()));

            tx.commit();
        }
    }

    private static void nodesWithPersonLabelHaveUniqueName(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            try (ResourceIterator<Node> person = tx.findNodes(label("Person"))) {
                Set<Object> names = new HashSet<>();
                while (person.hasNext()) {
                    Object name = person.next().getProperty("name", null);
                    if (name != null) {
                        Assertions.assertTrue(names.add(name), "non-unique name: " + name);
                    }
                }
            }

            tx.commit();
        }
    }

    // UTILITIES used for execution

    private static void createConstraint(GraphDatabaseService db) {
        if (USE_CYPHER) {
            db.executeTransactionally("create constraint for (p:Person) require p.name is unique");
        } else {
            try (Transaction tx = db.beginTx()) {
                tx.schema()
                        .constraintFor(label("Person"))
                        .assertPropertyIsUnique("name")
                        .create();

                tx.commit();
            }
        }
    }

    private static void addNode(GraphDatabaseService db) {
        if (USE_CYPHER) {
            db.executeTransactionally("create (:Person {name: 'Sneaky Steve'})");
        } else {
            try (Transaction tx = db.beginTx()) {
                tx.createNode(label("Person")).setProperty("name", "Sneaky Steve");

                tx.commit();
            }
        }
    }

    private static GraphDatabaseService graphdb(Path path) {
        managementService = new TestDatabaseManagementServiceBuilder(path).build();
        return managementService.database(DEFAULT_DATABASE_NAME);
    }

    private static void flushPageCache(GraphDatabaseService db) {
        try {
            ((GraphDatabaseAPI) db)
                    .getDependencyResolver()
                    .resolveDependency(PageCache.class)
                    .flushAndForce(DatabaseFlushEvent.NULL);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void await() throws IOException {
        System.in.read();
    }

    // PARAMETERIZATION

    private static Stream<Configuration> configurations() {
        List<Configuration> configurations = new ArrayList<>();
        if (EXHAUSTIVE) {
            for (int killSignal : KILL_SIGNALS) {
                configurations.add(
                        new Configuration().force_create_constraint(true).kill_signal(killSignal));
                configurations.add(
                        new Configuration().force_create_constraint(false).kill_signal(killSignal));
            }
        } else {
            configurations.add(new Configuration());
        }
        return configurations.stream();
    }

    public static class Configuration {
        boolean force_create_constraint;
        int kill_signal = 9;

        public Configuration force_create_constraint(boolean force_create_constraint) {
            this.force_create_constraint = force_create_constraint;
            return this;
        }

        public Configuration kill_signal(int kill_signal) {
            this.kill_signal = kill_signal;
            return this;
        }

        @Override
        public String toString() {
            return "Configuration{" + "use_cypher="
                    + USE_CYPHER + ", force_create_constraint="
                    + force_create_constraint + ", kill_signal="
                    + kill_signal + '}';
        }
    }

    // UTILITIES for process management

    private static String pidOf(Process process) throws Exception {
        return PID.get(process).toString();
    }

    private static void kill(int signal, Process process) throws Exception {
        int exitCode = new ProcessBuilder("/bin/sh", "-c", String.format("kill -%s %s", signal, pidOf(process)))
                .start()
                .waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("<kill -" + signal + "> failed, exit code: " + exitCode);
        }
    }

    private static Integer awaitMessage(Process process, String message) throws IOException, InterruptedException {
        BufferedReader out = new BufferedReader(new InputStreamReader(process.getInputStream()));
        for (String line; (line = out.readLine()) != null; ) {
            if (message != null && line.contains(message)) {
                return null;
            }
        }
        int exitCode = process.waitFor();
        BufferedReader err = new BufferedReader(new InputStreamReader(process.getInputStream()));
        for (String line; (line = out.readLine()) != null; ) {}
        for (String line; (line = err.readLine()) != null; ) {}
        return exitCode;
    }
}
