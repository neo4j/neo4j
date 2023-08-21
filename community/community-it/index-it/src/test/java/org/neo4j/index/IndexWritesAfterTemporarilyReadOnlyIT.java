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
package org.neo4j.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.fulltextSearch;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
class IndexWritesAfterTemporarilyReadOnlyIT {
    @Inject
    private TestDirectory directory;

    private final Label label = Label.label("Tag");
    private final String key = "key";
    private final String indexName = "Bob";

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexTypes")
    void shouldAllowWritesAfterTemporarilyReadOnlyDuringStartup(IndexType indexType, Value value)
            throws KernelException {
        assumeThat(indexType).isNotEqualTo(IndexType.LOOKUP);

        // given
        createIndexInIsolatedDbms(indexType);

        // when starting up this dbms again, although with the db set to (temporarily) read-only
        var dbms = dbmsBuilder()
                .setConfig(read_only_databases, Set.of(DEFAULT_DATABASE_NAME))
                .build();
        try {
            var db = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
            // just make sure it's available and doesn't accept writes
            try (var tx = db.beginTx()) {
                assertThatThrownBy(tx::createNode).isInstanceOf(WriteOperationsNotAllowedException.class);
            }

            // and when later making the db writable
            db.getDependencyResolver().resolveDependency(DatabaseConfig.class).set(read_only_databases, Set.of());

            // then it should be possible to make writes updating that index
            long nodeId;
            try (var tx = db.beginTx()) {
                var node = tx.createNode(label);
                node.setProperty(key, value.asObjectCopy());
                nodeId = node.getId();
                tx.commit();
            }
            try (var itx = db.beginTransaction(EXPLICIT, AUTH_DISABLED);
                    var cursor =
                            itx.kernelTransaction().cursors().allocateNodeValueIndexCursor(NULL_CONTEXT, INSTANCE)) {
                var ktx = itx.kernelTransaction();
                var index = ktx.schemaRead().indexGetForName(indexName);
                var session = ktx.dataRead().indexReadSession(index);
                var keyId = ktx.tokenRead().propertyKey(key);
                ktx.dataRead()
                        .nodeIndexSeek(
                                ktx.queryContext(), session, cursor, unconstrained(), query(indexType, keyId, value));
                assertThat(cursor.next()).isTrue();
                assertThat(cursor.nodeReference()).isEqualTo(nodeId);
                assertThat(cursor.next()).isFalse();
            }
        } finally {
            dbms.shutdown();
        }
    }

    private PropertyIndexQuery query(IndexType indexType, int keyId, Value value) {
        return switch (indexType) {
            case TEXT, RANGE -> exact(keyId, value);
            case FULLTEXT -> fulltextSearch(((TextValue) value).stringValue());
            case POINT -> {
                var pointValue = (PointValue) value;
                yield PropertyIndexQuery.boundingBox(keyId, pointValue, pointValue);
            }
            default -> throw new IllegalStateException("Unexpected value: " + indexType);
        };
    }

    private static Stream<Arguments> indexTypes() {
        var plainStringValue = Values.stringValue("abc");
        return Stream.of(
                arguments(IndexType.RANGE, plainStringValue),
                arguments(IndexType.TEXT, plainStringValue),
                arguments(IndexType.FULLTEXT, plainStringValue),
                arguments(IndexType.POINT, Values.pointValue(CoordinateReferenceSystem.WGS_84, 2D, 2D)));
    }

    private void createIndexInIsolatedDbms(IndexType indexType) {
        var dbms = dbmsBuilder().build();
        try {
            var db = dbms.database(DEFAULT_DATABASE_NAME);
            try (var tx = db.beginTx()) {
                tx.schema()
                        .indexFor(label)
                        .on(key)
                        .withIndexType(indexType)
                        .withName(indexName)
                        .create();
                tx.commit();
            }
        } finally {
            dbms.shutdown();
        }
    }

    private TestDatabaseManagementServiceBuilder dbmsBuilder() {
        return new TestDatabaseManagementServiceBuilder(directory.homePath());
    }
}
