/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.dbms.systemgraph;

import static java.time.Duration.ofSeconds;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.values.storable.DurationValue.duration;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.helpers.RemoteUri;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.logging.Level;

public class CommunityTopologyGraphDbmsModelIT extends BaseTopologyGraphDbmsModelIT {

    private CommunityTopologyGraphDbmsModel dbmsModel;

    @Override
    protected void createModel(Transaction tx) {
        dbmsModel = new CommunityTopologyGraphDbmsModel(tx);
    }

    @Test
    void canReturnAllInternalDatabaseReferences() {
        // given
        var fooDb = newDatabase(b -> b.withDatabase("foo"));
        var barDb = newDatabase(b -> b.withDatabase("bar"));
        createInternalReferenceForDatabase(tx, "fooAlias", false, fooDb);
        createInternalReferenceForDatabase(tx, "fooOtherAlias", false, fooDb);
        createInternalReferenceForDatabase(tx, "barAlias", false, barDb);

        var expected = Set.of(
                new DatabaseReference.Internal(new NormalizedDatabaseName("foo"), fooDb, true),
                new DatabaseReference.Internal(new NormalizedDatabaseName("bar"), barDb, true),
                new DatabaseReference.Internal(new NormalizedDatabaseName("fooAlias"), fooDb, false),
                new DatabaseReference.Internal(new NormalizedDatabaseName("fooOtherAlias"), fooDb, false),
                new DatabaseReference.Internal(new NormalizedDatabaseName("barAlias"), barDb, false));

        // when
        var aliases = dbmsModel.getAllInternalDatabaseReferences();

        // then
        assertThat(aliases).isEqualTo(expected);
    }

    @Test
    void canReturnAllExternalDatabaseReferences() {
        // given
        var remoteAddress = new SocketAddress("my.neo4j.com", 7687);
        var remoteNeo4j = new RemoteUri("neo4j", List.of(remoteAddress), null);
        var fooId = randomUUID();
        var fooOtherId = randomUUID();
        var barId = randomUUID();
        createExternalReferenceForDatabase(tx, "fooAlias", "foo", remoteNeo4j, fooId);
        createExternalReferenceForDatabase(tx, "fooOtherAlias", "foo", remoteNeo4j, fooOtherId);
        createExternalReferenceForDatabase(tx, "barAlias", "bar", remoteNeo4j, barId);

        var expected = Set.of(
                new DatabaseReference.External(
                        new NormalizedDatabaseName("foo"), new NormalizedDatabaseName("fooAlias"), remoteNeo4j, fooId),
                new DatabaseReference.External(
                        new NormalizedDatabaseName("foo"),
                        new NormalizedDatabaseName("fooOtherAlias"),
                        remoteNeo4j,
                        fooOtherId),
                new DatabaseReference.External(
                        new NormalizedDatabaseName("bar"), new NormalizedDatabaseName("barAlias"), remoteNeo4j, barId));

        // when
        var aliases = dbmsModel.getAllExternalDatabaseReferences();

        // then
        assertThat(aliases).isEqualTo(expected);
    }

    @Test
    void canReturnAllCompositeDatabaseReferences() {
        // given
        var remoteAddress = new SocketAddress("my.neo4j.com", 7687);
        var remoteNeo4j = new RemoteUri("neo4j", List.of(remoteAddress), null);
        var remAliasId1 = randomUUID();
        var remAliasId2 = randomUUID();
        var remAliasId3 = randomUUID();
        var remAliasId4 = randomUUID();
        var remAliasId5 = randomUUID();
        var locDb = newDatabase(b -> b.withDatabase("loc"));
        createInternalReferenceForDatabase(tx, "locAlias", false, locDb);
        createExternalReferenceForDatabase(tx, "remAlias", "rem1", remoteNeo4j, remAliasId1);
        var compDb1 = newDatabase(b -> b.withDatabase("compDb1").asVirtual());
        createInternalReferenceForDatabase(tx, compDb1.name(), true, compDb1);
        createInternalReferenceForDatabase(tx, compDb1.name(), "locAlias", false, locDb);
        createExternalReferenceForDatabase(tx, compDb1.name(), "remAlias", "rem2", remoteNeo4j, remAliasId2);
        createExternalReferenceForDatabase(tx, compDb1.name(), "remAlias2", "rem3", remoteNeo4j, remAliasId3);
        var compDb2 = newDatabase(b -> b.withDatabase("compDb2").asVirtual());
        createInternalReferenceForDatabase(tx, compDb2.name(), true, compDb2);
        createInternalReferenceForDatabase(tx, compDb2.name(), "locAlias", false, locDb);
        createExternalReferenceForDatabase(tx, compDb2.name(), "remAlias", "rem4", remoteNeo4j, remAliasId4);
        createExternalReferenceForDatabase(tx, compDb2.name(), "remAlias3", "rem5", remoteNeo4j, remAliasId5);

        // then
        assertThat(dbmsModel.getAllCompositeDatabaseReferences())
                .isEqualTo(Set.of(
                        new DatabaseReference.Composite(
                                new NormalizedDatabaseName(compDb1.name()),
                                compDb1,
                                Set.of(
                                        new DatabaseReference.Internal(
                                                new NormalizedDatabaseName("locAlias"), locDb, false),
                                        new DatabaseReference.External(
                                                new NormalizedDatabaseName("rem2"),
                                                new NormalizedDatabaseName("remAlias"),
                                                remoteNeo4j,
                                                remAliasId2),
                                        new DatabaseReference.External(
                                                new NormalizedDatabaseName("rem3"),
                                                new NormalizedDatabaseName("remAlias2"),
                                                remoteNeo4j,
                                                remAliasId3))),
                        new DatabaseReference.Composite(
                                new NormalizedDatabaseName(compDb2.name()),
                                compDb2,
                                Set.of(
                                        new DatabaseReference.Internal(
                                                new NormalizedDatabaseName("locAlias"), locDb, false),
                                        new DatabaseReference.External(
                                                new NormalizedDatabaseName("rem4"),
                                                new NormalizedDatabaseName("remAlias"),
                                                remoteNeo4j,
                                                remAliasId4),
                                        new DatabaseReference.External(
                                                new NormalizedDatabaseName("rem5"),
                                                new NormalizedDatabaseName("remAlias3"),
                                                remoteNeo4j,
                                                remAliasId5)))));

        assertThat(dbmsModel.getAllInternalDatabaseReferences())
                .isEqualTo(Set.of(
                        new DatabaseReference.Internal(new NormalizedDatabaseName("loc"), locDb, true),
                        new DatabaseReference.Internal(new NormalizedDatabaseName("locAlias"), locDb, false)));

        assertThat(dbmsModel.getAllExternalDatabaseReferences())
                .isEqualTo(Set.of(new DatabaseReference.External(
                        new NormalizedDatabaseName("rem1"),
                        new NormalizedDatabaseName("remAlias"),
                        remoteNeo4j,
                        remAliasId1)));
    }

    @Test
    void canReturnAllDatabaseReferences() {
        // given
        var fooDb = newDatabase(b -> b.withDatabase("foo"));
        createInternalReferenceForDatabase(tx, "fooAlias", false, fooDb);
        var remoteAddress = new SocketAddress("my.neo4j.com", 7687);
        var remoteNeo4j = new RemoteUri("neo4j", List.of(remoteAddress), null);
        var barId = UUID.randomUUID();
        var barId2 = UUID.randomUUID();
        createExternalReferenceForDatabase(tx, "bar", "foo", remoteNeo4j, barId);
        var compDb1 = newDatabase(b -> b.withDatabase("compDb1").asVirtual());
        createInternalReferenceForDatabase(tx, compDb1.name(), true, compDb1);
        createInternalReferenceForDatabase(tx, compDb1.name(), "locAlias", false, fooDb);
        createExternalReferenceForDatabase(tx, compDb1.name(), "remAlias", "rem", remoteNeo4j, barId2);

        var expected = Set.of(
                new DatabaseReference.External(
                        new NormalizedDatabaseName("foo"), new NormalizedDatabaseName("bar"), remoteNeo4j, barId),
                new DatabaseReference.Internal(new NormalizedDatabaseName("foo"), fooDb, true),
                new DatabaseReference.Internal(new NormalizedDatabaseName("fooAlias"), fooDb, false),
                new DatabaseReference.Composite(
                        new NormalizedDatabaseName(compDb1.name()),
                        compDb1,
                        Set.of(
                                new DatabaseReference.Internal(new NormalizedDatabaseName("locAlias"), fooDb, false),
                                new DatabaseReference.External(
                                        new NormalizedDatabaseName("rem"),
                                        new NormalizedDatabaseName("remAlias"),
                                        remoteNeo4j,
                                        barId2))));

        // when
        var aliases = dbmsModel.getAllDatabaseReferences();

        // then
        assertThat(aliases).isEqualTo(expected);
    }

    @Test
    void shouldReturnEmptyForDriverSettingsIfNoneExist() {
        // given
        var aliasName = "fooAlias";
        var remoteAddress = new SocketAddress("my.neo4j.com", 7687);
        var remoteNeo4j = new RemoteUri("neo4j", List.of(remoteAddress), null);
        createExternalReferenceForDatabase(tx, aliasName, "foo", remoteNeo4j, randomUUID());

        // when
        var result = dbmsModel.getDriverSettings(aliasName);

        // then
        assertThat(result).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("driverSettings")
    void canReturnDriverSettingsForExternalDatabaseReference(DriverSettings driverSettings) {
        // given
        var aliasName = "fooAlias";
        var remoteAddress = new SocketAddress("my.neo4j.com", 7687);
        var remoteNeo4j = new RemoteUri("neo4j", List.of(remoteAddress), null);
        var fooId = randomUUID();
        var aliasNode = createExternalReferenceForDatabase(tx, aliasName, "foo", remoteNeo4j, fooId);

        createDriverSettingsForExternalAlias(tx, aliasNode, driverSettings);

        // when
        var result = dbmsModel.getDriverSettings(aliasName);

        // then
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(driverSettings);
    }

    static Stream<Arguments> driverSettings() {
        var completeSettings = DriverSettings.builder()
                .withSslEnforced(true)
                .withConnectionTimeout(duration(ofSeconds(10)))
                .withConnectionPoolAcquisitionTimeout(duration(ofSeconds(1)))
                .withConnectionMaxLifeTime(duration(ofSeconds(300)))
                .withConnectionPoolIdleTest(duration(ofSeconds(1)))
                .withConnectionPoolMaxSize(0)
                .withLoggingLevel(Level.INFO)
                .build();

        var missingSettings = DriverSettings.builder()
                .withSslEnforced(false)
                .withLoggingLevel(Level.DEBUG)
                .build();

        var missingOtherSettings = DriverSettings.builder()
                .withConnectionTimeout(duration(ofSeconds(10)))
                .withConnectionPoolAcquisitionTimeout(duration(ofSeconds(1)))
                .withConnectionMaxLifeTime(duration(ofSeconds(300)))
                .withConnectionPoolIdleTest(duration(ofSeconds(1)))
                .build();

        return Stream.of(
                Arguments.of(completeSettings), Arguments.of(missingSettings), Arguments.of(missingOtherSettings));
    }
}
