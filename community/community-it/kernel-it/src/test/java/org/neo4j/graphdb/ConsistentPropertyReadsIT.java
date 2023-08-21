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
package org.neo4j.graphdb;

import static java.lang.Integer.max;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.test.Race;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

/**
 * Test for how properties are read and that they should be read consistently, i.e. adhere to neo4j's
 * interpretation of the ACID guarantees.
 */
@ImpermanentDbmsExtension
class ConsistentPropertyReadsIT {
    @Inject
    private GraphDatabaseService db;

    @Test
    void shouldReadConsistentPropertyValues() throws Throwable {
        // GIVEN
        var nodes = new Node[10];
        var keys = new String[] {"1", "2", "3"};
        var values = new String[] {
            longString('a'), longString('b'), longString('c'),
        };
        try (var tx = db.beginTx()) {
            for (var i = 0; i < nodes.length; i++) {
                nodes[i] = tx.createNode();
                for (String key : keys) {
                    nodes[i].setProperty(key, values[0]);
                }
            }
            tx.commit();
        }

        var numUpdaters = 2;
        var updatesDone = new AtomicLong();
        var readsDone = new AtomicLong();
        var race = new Race().withEndCondition(() -> updatesDone.get() > 1_000 && readsDone.get() > 100_000);
        race.addContestants(numUpdaters, () -> {
            var random = ThreadLocalRandom.current();
            var node = nodes[random.nextInt(nodes.length)];
            var key = keys[random.nextInt(keys.length)];
            try (var tx = db.beginTx()) {
                tx.getNodeById(node.getId()).removeProperty(key);
                tx.commit();
            }
            try (var tx = db.beginTx()) {
                tx.getNodeById(node.getId()).setProperty(key, values[random.nextInt(values.length)]);
                tx.commit();
            }
            updatesDone.incrementAndGet();
        });

        var numReaders = max(2, Runtime.getRuntime().availableProcessors() - numUpdaters);
        race.addContestants(numReaders, () -> {
            var random = ThreadLocalRandom.current();
            try (var tx = db.beginTx()) {
                var value = (String) tx.getNodeById(nodes[random.nextInt(nodes.length)].getId())
                        .getProperty(keys[random.nextInt(keys.length)], null);
                assertTrue(value == null || ArrayUtil.contains(values, value), value);
                tx.commit();
            }
            readsDone.incrementAndGet();
        });

        // WHEN
        race.go();
    }

    private static String longString(char c) {
        char[] chars = new char[ThreadLocalRandom.current().nextInt(800, 1000)];
        Arrays.fill(chars, c);
        return new String(chars);
    }
}
