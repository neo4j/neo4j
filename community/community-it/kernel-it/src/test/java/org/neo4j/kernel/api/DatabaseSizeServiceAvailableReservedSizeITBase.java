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
package org.neo4j.kernel.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;

import java.io.IOException;
import java.util.function.ToLongFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.database.DatabaseSizeService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ImpermanentDbmsExtension(configurationCallback = "configure")
@ExtendWith(RandomExtension.class)
public abstract class DatabaseSizeServiceAvailableReservedSizeITBase {
    protected static final String PROPERTY_KEY_PREFIX = "property";
    protected static final Label NODE_LABEL = Label.label("Label");
    protected static final RelationshipType REL_TYPE = RelationshipType.withName("RELATES_TO");

    @Inject
    GraphDatabaseAPI db;

    @Inject
    RandomSupport random;

    private DependencyResolver dependencyResolver;

    private final String format;

    protected DatabaseSizeServiceAvailableReservedSizeITBase(String format) {
        this.format = format;
    }

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(db_format, format);
    }

    @BeforeEach
    void setDependencyResolver() {
        dependencyResolver = db.getDependencyResolver();
    }

    protected void assertAvailableReservedSpaceChanged(ToLongFunction<Transaction> operation) throws IOException {
        final var sizeService = db.getDependencyResolver().resolveDependency(DatabaseSizeService.class);
        final var before = sizeService.getDatabaseAvailableReservedSize(db.databaseId());
        long expectedDifference;
        try (final var tx = db.beginTx()) {
            expectedDifference = operation.applyAsLong(tx);
            tx.commit();
        }
        final var after = sizeService.getDatabaseAvailableReservedSize(db.databaseId());
        assertThat(after - before).isEqualTo(expectedDifference);
    }

    protected <T> T get(Class<T> cls) {
        return dependencyResolver.resolveDependency(cls);
    }
}
