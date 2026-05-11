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
package org.neo4j.tooling.procedure;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.tooling.procedure.procedures.valid.Procedures;

@DbmsExtension(configurationCallback = "configure")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProcedureTest {
    private static final Class<?> PROCEDURES_CLASS = Procedures.class;
    private static final String procedureNamespace =
            PROCEDURES_CLASS.getPackage().getName();

    @Inject
    GraphDatabaseAPI db;

    @Inject
    GlobalProcedures globalProcedures;

    @Inject
    ConnectorPortRegister connectorPortRegister;

    private URI boltURI;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(BoltConnector.enabled, true);
    }

    @BeforeAll
    void setUp() throws Exception {
        globalProcedures.registerProcedure(PROCEDURES_CLASS);
        boltURI = URI.create("bolt://"
                + connectorPortRegister.getLocalAddress(ConnectorType.BOLT).toString("localhost"));
    }

    @Test
    void callsSimplisticProcedure() {
        try (Driver driver = GraphDatabase.driver(boltURI, configuration());
                Session session = driver.session()) {

            Result result = session.run("CALL " + procedureNamespace + ".theAnswer()");

            assertThat(result.single().get("value").asLong()).isEqualTo(42L);
        }
    }

    @Test
    void callsProceduresWithSimpleInputTypeReturningVoid() {
        try (Driver driver = GraphDatabase.driver(boltURI, configuration());
                Session session = driver.session()) {

            session.run("CALL " + procedureNamespace + ".simpleInput00()");
            session.run("CALL " + procedureNamespace + ".simpleInput01('string')");
            session.run("CALL " + procedureNamespace + ".simpleInput02(42)");
            session.run("CALL " + procedureNamespace + ".simpleInput03(42)");
            session.run("CALL " + procedureNamespace + ".simpleInput04(4.2)");
            session.run("CALL " + procedureNamespace + ".simpleInput05(true)");
            session.run("CALL " + procedureNamespace + ".simpleInput06(false)");
            session.run("CALL " + procedureNamespace + ".simpleInput07({foo:'bar'})");
            session.run("MATCH (n)            CALL " + procedureNamespace + ".simpleInput08(n) RETURN n");
            session.run("MATCH p=(()-[r]->()) CALL " + procedureNamespace + ".simpleInput09(p) RETURN p");
            session.run("MATCH ()-[r]->()     CALL " + procedureNamespace + ".simpleInput10(r) RETURN r");
        }
    }

    @Test
    void callsProceduresWithDifferentModesReturningVoid() {
        try (Driver driver = GraphDatabase.driver(boltURI, configuration());
                Session session = driver.session()) {
            session.run("CALL " + procedureNamespace + ".defaultMode()");
            session.run("CALL " + procedureNamespace + ".readMode()");
            session.run("CALL " + procedureNamespace + ".writeMode()");
            session.run("CALL " + procedureNamespace + ".schemaMode()");
            session.run("CALL " + procedureNamespace + ".dbmsMode()");
        }
    }

    @Test
    void callsProceduresWithSimpleInputTypeReturningRecordWithPrimitiveFields() {
        try (Driver driver = GraphDatabase.driver(boltURI, configuration());
                Session session = driver.session()) {
            assertThat(session.run("CALL " + procedureNamespace
                                    + ".simpleInput11('string') YIELD field04 AS p RETURN p")
                            .single())
                    .isNotNull();
            assertThat(session.run("CALL " + procedureNamespace + ".simpleInput12(42)")
                            .single())
                    .isNotNull();
            assertThat(session.run("CALL " + procedureNamespace + ".simpleInput13(42)")
                            .single())
                    .isNotNull();
            assertThat(session.run("CALL " + procedureNamespace + ".simpleInput14(4.2)")
                            .single())
                    .isNotNull();
            assertThat(session.run("CALL " + procedureNamespace + ".simpleInput15(true)")
                            .single())
                    .isNotNull();
            assertThat(session.run("CALL " + procedureNamespace + ".simpleInput16(false)")
                            .single())
                    .isNotNull();
            assertThat(session.run("CALL " + procedureNamespace + ".simpleInput17({foo:'bar'})")
                            .single())
                    .isNotNull();
            assertThat(session.run("CALL " + procedureNamespace + ".simpleInput21()")
                            .single())
                    .isNotNull();
        }
    }

    private Config configuration() {
        return Config.builder()
                .withoutEncryption()
                .withTelemetryDisabled(true)
                .withLogging(Logging.none())
                .withConnectionTimeout(10, TimeUnit.SECONDS)
                .withAutoCommitRetriesDisabled(true)
                .build();
    }
}
