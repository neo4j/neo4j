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
package org.neo4j.dbms.systemgraph;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DEFAULT_NAMESPACE;
import static org.neo4j.kernel.database.DatabaseReferenceImpl.PropertyShard.propertyShardName;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.helpers.RemoteUri;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.NormalizedCatalogEntry;
import org.neo4j.kernel.database.NormalizedDatabaseName;

public class CommunityTopologyGraphDbmsModelIT extends BaseTopologyGraphDbmsModelIT {
    private CommunityTopologyGraphDbmsModel dbmsModel;

    @Override
    protected void createModel(Transaction tx) {
        dbmsModel = new CommunityTopologyGraphDbmsModel(tx);
    }

    protected TopologyGraphDbmsModel dbmsModel() {
        return dbmsModel;
    }

    protected String mirrorUpstream() {
        return "nothing reads this in community";
    }

    private static NormalizedDatabaseName name(String name) {
        return new NormalizedDatabaseName(name);
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
        createExternalReferenceForDatabase(tx, "remAlias", "rem1", remoteNeo4j, remAliasId1, false);
        var compDb1 = newDatabase(b -> b.withDatabase("compDb1").asComposite());
        var compDb1Name = compDb1.normalizedName();
        createInternalReferenceForDatabase(tx, compDb1.name(), true, compDb1);
        createInternalReferenceForDatabase(tx, compDb1.name(), "locAlias", false, locDb);
        createExternalReferenceForDatabase(tx, compDb1.name(), "remAlias", "rem2", remoteNeo4j, remAliasId2, false);
        createExternalReferenceForDatabase(tx, compDb1.name(), "remAlias2", "rem3", remoteNeo4j, remAliasId3, false);
        var compDb2 = newDatabase(b -> b.withDatabase("compDb2").asComposite());
        var compDb2Name = compDb2.normalizedName();
        createInternalReferenceForDatabase(tx, compDb2.name(), true, compDb2);
        createInternalReferenceForDatabase(tx, compDb2.name(), "locAlias", false, locDb);
        createExternalReferenceForDatabase(tx, compDb2.name(), "remAlias", "rem4", remoteNeo4j, remAliasId4, false);
        createExternalReferenceForDatabase(tx, compDb2.name(), "remAlias3", "rem5", remoteNeo4j, remAliasId5, true);

        // then
        var comp1Ref = new DatabaseReferenceImpl.Composite(
                compDb1Name,
                compDb1,
                Set.of(
                        new DatabaseReferenceImpl.Internal(name("locAlias"), compDb1Name, locDb, false),
                        new DatabaseReferenceImpl.External(
                                name("rem2"), name("remAlias"), compDb1Name, remoteNeo4j, remAliasId2, false),
                        new DatabaseReferenceImpl.External(
                                name("rem3"), name("remAlias2"), compDb1Name, remoteNeo4j, remAliasId3, false)));
        var comp2Ref = new DatabaseReferenceImpl.Composite(
                compDb2Name,
                compDb2,
                Set.of(
                        new DatabaseReferenceImpl.Internal(name("locAlias"), compDb2Name, locDb, false),
                        new DatabaseReferenceImpl.External(
                                name("rem4"), name("remAlias"), compDb2Name, remoteNeo4j, remAliasId4, false),
                        new DatabaseReferenceImpl.External(
                                name("rem5"), name("remAlias3"), compDb2Name, remoteNeo4j, remAliasId5, true)));

        assertThat(dbmsModel().getAllCompositeDatabaseReferences()).isEqualTo(Set.of(comp1Ref, comp2Ref));

        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(compDb1.name())))
                .hasValue(comp1Ref);
        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(compDb2.name())))
                .hasValue(comp2Ref);
        // since no reference was explicitly created this is empty - this in artefact of the test setup
        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(locDb.name())))
                .isEmpty();
        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry("locAlias")))
                .hasValue(new DatabaseReferenceImpl.Internal(name("locAlias"), locDb, false));
        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry("remAlias")))
                .hasValue(new DatabaseReferenceImpl.External(
                        name("rem1"), name("remAlias"), remoteNeo4j, remAliasId1, false));
    }

    @Test
    void canReturnAllShardedPropertyDatabaseReferences() {
        // given
        String foo = "foo";
        var fooG = newDatabase(b -> b.withDatabase(DatabaseReferenceImpl.GraphShard.graphShardName(foo)));
        var fooP0 = newDatabase(b -> b.withDatabase(propertyShardName(foo, 0)));
        var fooP1 = newDatabase(b -> b.withDatabase(propertyShardName(foo, 1)));
        var fooNamedDatabase =
                createSpdReferenceForDatabase(tx, foo, fooG, List.of(Pair.of(0, fooP0), Pair.of(1, fooP1)));

        String bar = "bar";
        var barP0 = newDatabase(b -> b.withDatabase(propertyShardName(bar, 0)));
        var barP1 = newDatabase(b -> b.withDatabase(propertyShardName(bar, 1)));
        var barP2 = newDatabase(b -> b.withDatabase(propertyShardName(bar, 2)));
        var barP3 = newDatabase(b -> b.withDatabase(propertyShardName(bar, 3)));
        var barG = newDatabase(b -> b.withDatabase(DatabaseReferenceImpl.GraphShard.graphShardName(bar)));
        var barNamedDatabase = createSpdReferenceForDatabase(
                tx, bar, barG, List.of(Pair.of(0, barP0), Pair.of(1, barP1), Pair.of(2, barP2), Pair.of(3, barP3)));

        // then
        var fooP0Ref = new DatabaseReferenceImpl.PropertyShard(fooP0.normalizedName(), fooP0, foo, 0);
        var fooP1Ref = new DatabaseReferenceImpl.PropertyShard(fooP1.normalizedName(), fooP1, foo, 1);
        var fooShards = Map.of(
                0, fooP0Ref,
                1, fooP1Ref);
        var fooGRef = new DatabaseReferenceImpl.GraphShard(fooG.normalizedName(), fooG, "foo", fooShards);
        var fooSpdRef =
                new DatabaseReferenceImpl.VirtualSPD(new NormalizedDatabaseName(foo), fooNamedDatabase, fooGRef, true);

        var barP0Ref = new DatabaseReferenceImpl.PropertyShard(barP0.normalizedName(), barP0, bar, 0);
        var barP1Ref = new DatabaseReferenceImpl.PropertyShard(barP1.normalizedName(), barP1, bar, 1);
        var barP2Ref = new DatabaseReferenceImpl.PropertyShard(barP2.normalizedName(), barP2, bar, 2);
        var barP3Ref = new DatabaseReferenceImpl.PropertyShard(barP3.normalizedName(), barP3, bar, 3);
        var barShards = Map.of(
                0, barP0Ref,
                1, barP1Ref,
                2, barP2Ref,
                3, barP3Ref);
        var barGRef = new DatabaseReferenceImpl.GraphShard(barG.normalizedName(), barG, "bar", barShards);
        var barSpdRef = new DatabaseReferenceImpl.VirtualSPD(name(bar), barNamedDatabase, barGRef, true);

        assertThat(dbmsModel().getAllDatabaseReferences())
                .containsExactlyInAnyOrder(
                        fooSpdRef, fooGRef, fooP0Ref, fooP1Ref, barSpdRef, barGRef, barP0Ref, barP1Ref, barP2Ref,
                        barP3Ref);
        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(foo)))
                .hasValue(fooSpdRef);
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName(foo)))
                .hasValue(fooSpdRef);
        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(bar)))
                .hasValue(barSpdRef);
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName(bar)))
                .hasValue(barSpdRef);

        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(fooG.name())))
                .hasValue(fooSpdRef.graphShard());
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName(fooG.name())))
                .hasValue(fooSpdRef.graphShard());

        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(fooG.name())))
                .hasValue(fooGRef);
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName(fooG.name())))
                .hasValue(fooGRef);

        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(fooP0.name())))
                .hasValue(fooSpdRef.graphShard().propertyShards().get(0));
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName(fooP0.name())))
                .hasValue(fooSpdRef.graphShard().propertyShards().get(0));

        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(fooP1.name())))
                .hasValue(fooSpdRef.graphShard().propertyShards().get(1));
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName(fooP1.name())))
                .hasValue(fooSpdRef.graphShard().propertyShards().get(1));

        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(barG.name())))
                .hasValue(barSpdRef.graphShard());
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName(barG.name())))
                .hasValue(barSpdRef.graphShard());

        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(barG.name())))
                .hasValue(barGRef);
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName(barG.name())))
                .hasValue(barGRef);

        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(barP0.name())))
                .hasValue(barSpdRef.graphShard().propertyShards().get(0));
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName(barP0.name())))
                .hasValue(barSpdRef.graphShard().propertyShards().get(0));

        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(barP1.name())))
                .hasValue(barSpdRef.graphShard().propertyShards().get(1));
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName(barP1.name())))
                .hasValue(barSpdRef.graphShard().propertyShards().get(1));

        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(barP2.name())))
                .hasValue(barSpdRef.graphShard().propertyShards().get(2));
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName(barP2.name())))
                .hasValue(barSpdRef.graphShard().propertyShards().get(2));

        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(barP3.name())))
                .hasValue(barSpdRef.graphShard().propertyShards().get(3));
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName(barP3.name())))
                .hasValue(barSpdRef.graphShard().propertyShards().get(3));
    }

    @Test
    void canReturnAllMirrorDatabaseReferences() {
        // given
        var mirror0 = newDatabase(b -> b.withDatabase("mirror0").withUpstream(mirrorUpstream()));
        var mirror1 = newDatabase(b -> b.withDatabase("mirror1").withUpstream(mirrorUpstream()));
        createInternalReferenceForDatabase(tx, mirror0.name(), true, mirror0);
        createInternalReferenceForDatabase(tx, mirror1.name(), true, mirror1);

        // then
        var mirror0Ref = new DatabaseReferenceImpl.Mirror(mirror0.normalizedName(), mirror0);
        var mirror1Ref = new DatabaseReferenceImpl.Mirror(mirror1.normalizedName(), mirror1);

        assertThat(dbmsModel().getAllDatabaseReferences()).containsExactlyInAnyOrder(mirror0Ref, mirror1Ref);
        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(mirror0.name())))
                .hasValue(mirror0Ref);
        assertThat(dbmsModel().getDatabaseRefByAlias(new NormalizedCatalogEntry(mirror1.name())))
                .hasValue(mirror1Ref);
    }

    @Test
    void returnsBestReference() {
        var real0Db = newDatabase(b -> b.withDatabase("real0"));
        var real1Db = newDatabase(b -> b.withDatabase("real1"));
        var real2Db = newDatabase(b -> b.withDatabase("real2"));
        var real3Db = newDatabase(b -> b.withDatabase("real3"));

        var comp00Db = newDatabase(b -> b.withDatabase("golf.hotel.india").asComposite());
        createInternalReferenceForDatabase(tx, comp00Db.name(), true, comp00Db);
        createInternalReferenceForDatabase(tx, comp00Db.name(), "juliet", false, real0Db);

        assertThat(dbmsModel().getDatabaseRefByAlias("golf.hotel.india.juliet"))
                .map(DatabaseReference::namedDatabaseId)
                .hasValue(real0Db);

        var comp01Db = newDatabase(b -> b.withDatabase("golf.hotel").asComposite());
        createInternalReferenceForDatabase(tx, comp01Db.name(), true, comp01Db);
        createInternalReferenceForDatabase(tx, comp01Db.name(), "india.juliet", false, real1Db);

        assertThat(dbmsModel().getDatabaseRefByAlias("golf.hotel.india.juliet"))
                .map(DatabaseReference::namedDatabaseId)
                .hasValue(real1Db);

        var comp02Db = newDatabase(b -> b.withDatabase("golf").asComposite());
        createInternalReferenceForDatabase(tx, comp02Db.name(), true, comp02Db);
        createInternalReferenceForDatabase(tx, comp02Db.name(), "hotel.india.juliet", false, real2Db);

        assertThat(dbmsModel().getDatabaseRefByAlias("golf.hotel.india.juliet"))
                .map(DatabaseReference::namedDatabaseId)
                .hasValue(real2Db);

        createInternalReferenceForDatabase(tx, "golf.hotel.india.juliet", false, real3Db);

        assertThat(dbmsModel().getDatabaseRefByAlias("golf.hotel.india.juliet"))
                .map(DatabaseReference::namedDatabaseId)
                .hasValue(real3Db);
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
        createExternalReferenceForDatabase(tx, "bar", "foo", remoteNeo4j, barId, false);
        var compDb1 = newDatabase(b -> b.withDatabase("compDb1").asComposite());
        var compDb1Name = compDb1.normalizedName();
        createInternalReferenceForDatabase(tx, compDb1.name(), true, compDb1);
        createInternalReferenceForDatabase(tx, compDb1.name(), "locAlias", false, fooDb);
        createExternalReferenceForDatabase(tx, compDb1.name(), "remAlias", "rem", remoteNeo4j, barId2, false);

        var db = new DatabaseReferenceImpl.Internal(name("foo"), fooDb, true);
        var dbAlias = new DatabaseReferenceImpl.Internal(name("fooAlias"), fooDb, false);
        var remoteAlias = new DatabaseReferenceImpl.External(name("foo"), name("bar"), remoteNeo4j, barId, false);
        var localConstituent = new DatabaseReferenceImpl.Internal(name("locAlias"), compDb1Name, fooDb, false);
        var remoteConstituent = new DatabaseReferenceImpl.External(
                name("rem"), name("remAlias"), compDb1Name, remoteNeo4j, barId2, false);
        var composite = new DatabaseReferenceImpl.Composite(
                compDb1.normalizedName(), compDb1, Set.of(localConstituent, remoteConstituent));
        var expected = Set.of(remoteAlias, db, dbAlias, composite);

        // then
        var aliases = dbmsModel().getAllDatabaseReferences();
        assertThat(aliases).isEqualTo(expected);

        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName("fooAlias")))
                .contains(dbAlias);
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName("bar")))
                .contains(remoteAlias);
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName("compDb1")))
                .contains(composite);
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName("compDb1.locAlias")))
                .contains(localConstituent);
        assertThat(dbmsModel().getDatabaseRefByDisplayName(new NormalizedDatabaseName("compDb1.remAlias")))
                .contains(remoteConstituent);
    }

    @Test
    void shouldReturnEmptyForDriverSettingsIfNoneExist() {
        // given
        var aliasName = "fooAlias";
        var remoteAddress = new SocketAddress("my.neo4j.com", 7687);
        var remoteNeo4j = new RemoteUri("neo4j", List.of(remoteAddress), null);
        createExternalReferenceForDatabase(tx, aliasName, "foo", remoteNeo4j, randomUUID(), false);

        // when
        var result = dbmsModel().getDriverSettings(aliasName, DEFAULT_NAMESPACE);

        // then
        assertThat(result).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("driverSettings")
    void canReturnDriverSettingsForExternalDatabaseReference(String ignore, DriverSettings driverSettings) {
        // given
        var aliasName = "fooAlias";
        var remoteAddress = new SocketAddress("my.neo4j.com", 7687);
        var remoteNeo4j = new RemoteUri("neo4j", List.of(remoteAddress), null);
        var fooId = randomUUID();
        var aliasNode = createExternalReferenceForDatabase(tx, aliasName, "foo", remoteNeo4j, fooId, false);

        createDriverSettingsForExternalAlias(tx, aliasNode, driverSettings);

        // when
        var result = dbmsModel().getDriverSettings(aliasName, DEFAULT_NAMESPACE);

        // then
        assertThat(result).isPresent();
        assertThat(result).contains(driverSettings);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("driverSettings")
    void canReturnDriverSettingsForExternalDatabaseReferenceForwardingOidcTokens(
            String ignore, DriverSettings driverSettings) {
        // given
        var aliasName = "fooAlias";
        var remoteAddress = new SocketAddress("my.neo4j.com", 7687);
        var remoteNeo4j = new RemoteUri("neo4j", List.of(remoteAddress), null);
        var fooId = randomUUID();
        var aliasNode = createExternalReferenceForDatabase(tx, aliasName, "foo", remoteNeo4j, fooId, true);

        createDriverSettingsForExternalAlias(tx, aliasNode, driverSettings);

        // when
        var result = dbmsModel().getDriverSettings(aliasName, DEFAULT_NAMESPACE);

        // then
        assertThat(result).isPresent();
        assertThat(result).contains(driverSettings);
    }

    @Test
    void shouldReturnExternalDatabaseCredentialsForExternalDatabaseReference() {
        // given
        var aliasName = "fooAlias";
        var remoteAddress = new SocketAddress("my.neo4j.com", 7687);
        var remoteNeo4j = new RemoteUri("neo4j", List.of(remoteAddress), null);
        var fooId = randomUUID();
        createExternalReferenceForDatabase(tx, aliasName, "foo", remoteNeo4j, fooId, false);

        // when
        var result = dbmsModel()
                .getExternalDatabaseCredentials(
                        new DatabaseReferenceImpl.External(name("foo"), name(aliasName), remoteNeo4j, fooId, false));

        // then
        assertThat(result).isPresent();
        assertThat(result.map(ExternalDatabaseCredentials::username)).hasValue("username");
        assertThat(result.map(ExternalDatabaseCredentials::password)).hasValue("password".getBytes());
        assertThat(result.map(ExternalDatabaseCredentials::iv)).hasValue("i_vector".getBytes());
    }

    @Test
    void shouldNotReturnExternalDatabaseCredentialsForExternalDatabaseReferenceForwardingOidcTokens() {
        // given
        var aliasName = "fooAlias";
        var remoteAddress = new SocketAddress("my.neo4j.com", 7687);
        var remoteNeo4j = new RemoteUri("neo4j", List.of(remoteAddress), null);
        var fooId = randomUUID();
        createExternalReferenceForDatabase(tx, aliasName, "foo", remoteNeo4j, fooId, true);

        // when
        var result = dbmsModel()
                .getExternalDatabaseCredentials(
                        new DatabaseReferenceImpl.External(name("foo"), name(aliasName), remoteNeo4j, fooId, true));

        // then
        assertThat(result).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("aliasProperties")
    void canReturnPropertiesForExternalDatabaseReference(Map<String, Object> properties) {
        // given
        var aliasName = "fooAlias";
        var aliasNamespace = "composite";
        var remoteAddress = new SocketAddress("my.neo4j.com", 7687);
        var remoteNeo4j = new RemoteUri("neo4j", List.of(remoteAddress), null);
        var fooId = randomUUID();
        var aliasNode =
                createExternalReferenceForDatabase(tx, aliasNamespace, aliasName, "foo", remoteNeo4j, fooId, false);

        createPropertiesForAlias(tx, aliasNode, properties);

        // when
        var result = dbmsModel().getAliasProperties(aliasName, aliasNamespace);

        // then
        assertThat(result).isPresent();
        assertThat(result).contains(properties);
    }

    @ParameterizedTest
    @MethodSource("aliasProperties")
    void canReturnPropertiesForExternalDatabaseReferenceForwardingOidcTokens(Map<String, Object> properties) {
        // given
        var aliasName = "fooAlias";
        var aliasNamespace = "composite";
        var remoteAddress = new SocketAddress("my.neo4j.com", 7687);
        var remoteNeo4j = new RemoteUri("neo4j", List.of(remoteAddress), null);
        var fooId = randomUUID();
        var aliasNode =
                createExternalReferenceForDatabase(tx, aliasNamespace, aliasName, "foo", remoteNeo4j, fooId, true);

        createPropertiesForAlias(tx, aliasNode, properties);

        // when
        var result = dbmsModel().getAliasProperties(aliasName, aliasNamespace);

        // then
        assertThat(result).isPresent();
        assertThat(result).contains(properties);
    }

    @ParameterizedTest
    @MethodSource("aliasProperties")
    void canReturnPropertiesForInternalDatabaseReference(Map<String, Object> properties) {
        // given
        var aliasName = "fooAlias";
        var aliasNamespace = "composite";
        var locDb = newDatabase(b -> b.withDatabase("loc"));
        var aliasNode = createInternalReferenceForDatabase(tx, aliasNamespace, aliasName, false, locDb);

        createPropertiesForAlias(tx, aliasNode, properties);

        // when
        var result = dbmsModel().getAliasProperties(aliasName, aliasNamespace);

        // then
        assertThat(result).isPresent();
        assertThat(result).contains(properties);
    }

    @Test
    void canReturnRemoteAliasDefaultLanguage() {
        var remoteAddress = new SocketAddress("my.neo4j.com", 7687);
        var remoteNeo4j = new RemoteUri("neo4j", List.of(remoteAddress), null);
        var remAliasId1 = randomUUID();
        var remAliasId2 = randomUUID();
        createExternalReferenceForDatabase(tx, "remote1", "rem1", remoteNeo4j, remAliasId1, false);
        createExternalReferenceForDatabase(
                tx, "remote2", "rem1", remoteNeo4j, remAliasId2, CypherVersion.Cypher25, false);
        var locDb = newDatabase(b -> b.withDatabase("loc"));
        createInternalReferenceForDatabase(tx, "locAlias", false, locDb);

        assertThat(dbmsModel().getRemoteAliasLanguageVersion("remote1")).hasValue(CypherVersion.Cypher5);
        assertThat(dbmsModel().getRemoteAliasLanguageVersion("remote2")).hasValue(CypherVersion.Cypher25);

        assertThat(dbmsModel().getRemoteAliasLanguageVersion("nonExisting")).isEmpty();
        assertThat(dbmsModel().getRemoteAliasLanguageVersion("locDb")).isEmpty();
        assertThat(dbmsModel().getRemoteAliasLanguageVersion("locAlias")).isEmpty();
    }
}
