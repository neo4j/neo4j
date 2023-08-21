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

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.junit.extension.Neo4jExtension;
import org.neo4j.tooling.procedure.procedures.valid.Procedures;

class ProcedureTest {
    private static final Class<?> PROCEDURES_CLASS = Procedures.class;

    @RegisterExtension
    static Neo4jExtension graphDb =
            Neo4jExtension.builder().withProcedure(PROCEDURES_CLASS).build();

    private static final String procedureNamespace =
            PROCEDURES_CLASS.getPackage().getName();

    @Test
    void callsSimplisticProcedure(Neo4j neo4j) {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), configuration());
                Session session = driver.session()) {

            Result result = session.run("CALL " + procedureNamespace + ".theAnswer()");

            assertThat(result.single().get("value").asLong()).isEqualTo(42L);
        }
    }

    @Test
    void callsProceduresWithSimpleInputTypeReturningVoid(Neo4j neo4j) {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), configuration());
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
    void callsProceduresWithDifferentModesReturningVoid(Neo4j neo4j) {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), configuration());
                Session session = driver.session()) {
            session.run("CALL " + procedureNamespace + ".defaultMode()");
            session.run("CALL " + procedureNamespace + ".readMode()");
            session.run("CALL " + procedureNamespace + ".writeMode()");
            session.run("CALL " + procedureNamespace + ".schemaMode()");
            session.run("CALL " + procedureNamespace + ".dbmsMode()");
        }
    }

    @Test
    void callsProceduresWithSimpleInputTypeReturningRecordWithPrimitiveFields(Neo4j neo4j) {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), configuration());
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
                .withLogging(Logging.none())
                .withConnectionTimeout(10, TimeUnit.SECONDS)
                .build();
    }
}
