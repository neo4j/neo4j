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
package org.neo4j.fabric;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.driver.Driver;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.reactive.ReactiveTransaction;
import org.neo4j.test.extension.BoltDbmsExtension;
import org.neo4j.test.extension.Inject;
import reactor.core.publisher.Mono;

@BoltDbmsExtension
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BoltLocalResultStreamTest {

    @Inject
    private static ConnectorPortRegister connectorPortRegister;

    private static Driver driver;

    @BeforeAll
    static void beforeAll() {
        driver = DriverUtils.createDriver(connectorPortRegister);
    }

    @AfterAll
    static void tearDown() {
        driver.close();
    }

    @Test
    void testBasicResultStream() {
        List<String> result = inTx(tx -> tx.run("UNWIND range(0, 4) AS i RETURN 'r' + i as A").stream()
                .map(r -> r.get("A").asString())
                .collect(Collectors.toList()));
        assertThat(result).isEqualTo(List.of("r0", "r1", "r2", "r3", "r4"));
    }

    @Test
    void testRxResultStream() {
        List<String> result =
                inRxTx(tx -> Mono.fromDirect(flowPublisherToFlux(tx.run("UNWIND range(0, 4) AS i RETURN 'r' + i as A")))
                        .flatMapMany(reactiveResult -> flowPublisherToFlux(reactiveResult.records()))
                        .limitRate(1)
                        .collectList()
                        .block()
                        .stream()
                        .map(r -> r.get("A").asString())
                        .collect(Collectors.toList()));

        assertThat(result).isEqualTo(List.of("r0", "r1", "r2", "r3", "r4"));
    }

    @Test
    void testPartialStream() {
        List<String> result =
                inRxTx(tx -> Mono.fromDirect(flowPublisherToFlux(tx.run("UNWIND range(0, 4) AS i RETURN 'r' + i as A")))
                        .flatMapMany(reactiveResult -> flowPublisherToFlux(reactiveResult.records()))
                        .limitRequest(2)
                        .collectList()
                        .block()
                        .stream()
                        .map(r -> r.get("A").asString())
                        .collect(Collectors.toList()));

        assertThat(result).isEqualTo(List.of("r0", "r1"));
    }

    private static <T> T inTx(Function<TransactionContext, T> workload) {
        try (var session = driver.session()) {
            return session.executeWrite(workload::apply);
        }
    }

    private static <T> T inRxTx(Function<ReactiveTransaction, T> workload) {
        var session = driver.reactiveSession();
        try {
            ReactiveTransaction tx =
                    Mono.from(flowPublisherToFlux(session.beginTransaction())).block();
            try {
                return workload.apply(tx);
            } finally {
                Mono.from(flowPublisherToFlux(tx.rollback())).block();
            }
        } finally {
            Mono.from(flowPublisherToFlux(session.close())).block();
        }
    }
}
