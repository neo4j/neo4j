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
package org.neo4j.configuration;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.configuration.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.configuration.GraphDatabaseSettings.default_language;
import static org.neo4j.configuration.GraphDatabaseSettings.default_listen_address;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_sampling_percentage;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_tracing_level;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class GraphDatabaseSettingsTest {
    @Inject
    private TestDirectory directory;

    @Test
    void mustHaveNullDefaultPageCacheMemorySizeInBytes() {
        Long bytes = Config.defaults().get(GraphDatabaseSettings.pagecache_memory);
        assertThat(bytes).isNull();
    }

    @Test
    void pageCacheSettingMustAcceptArbitraryUserSpecifiedValue() throws IOException {
        assertPageCacheMemorySettingIsParsedAsBytes("245760");
        assertPageCacheMemorySettingIsParsedAsBytes("2244g");
        assertPageCacheMemorySettingIsParsedAsBytes("8m");
    }

    private void assertPageCacheMemorySettingIsParsedAsBytes(String value) throws IOException {
        Path cfg = directory.file("cfg");
        Files.writeString(
                cfg,
                GraphDatabaseSettings.pagecache_memory.name() + "=" + value,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        Config config = Config.newBuilder().fromFile(cfg).build();
        assertThat(config.get(GraphDatabaseSettings.pagecache_memory)).isEqualTo(ByteUnit.parse(value));
    }

    @Test
    void noDuplicateSettingsAreAllowed() throws Exception {
        final Map<String, String> fields = new HashMap<>();
        for (Field field : GraphDatabaseSettings.class.getDeclaredFields()) {
            if (field.getType() == Setting.class) {
                Setting<?> setting = (Setting<?>) field.get(null);

                assertThat(fields)
                        .as(format(
                                "'%s' in %s has already been defined in %s",
                                setting.name(), field.getName(), fields.get(setting.name())))
                        .doesNotContainKey(setting.name());
                fields.put(setting.name(), field.getName());
            }
        }
    }

    @Test
    void shouldEnableBoltByDefault() {
        // given
        Config config = Config.newBuilder()
                .setDefaults(GraphDatabaseSettings.SERVER_DEFAULTS)
                .build();

        // when
        SocketAddress listenSocketAddress = config.get(BoltConnector.listen_address);

        // then
        assertThat(listenSocketAddress).isEqualTo(new SocketAddress("localhost", 7687));
    }

    @Test
    void shouldBeAbleToOverrideBoltListenAddressesWithJustOneParameter() {
        // given
        Config config = Config.newBuilder()
                .set(BoltConnector.enabled, true)
                .set(BoltConnector.listen_address, new SocketAddress(8000))
                .build();

        // then
        assertThat(config.get(BoltConnector.listen_address)).isEqualTo(new SocketAddress("localhost", 8000));
    }

    @Test
    void shouldDeriveBoltListenAddressFromDefaultListenAddress() {
        // given
        Config config = Config.newBuilder()
                .set(BoltConnector.enabled, true)
                .set(GraphDatabaseSettings.default_listen_address, new SocketAddress("0.0.0.0"))
                .build();

        // then
        assertThat(config.get(BoltConnector.listen_address)).isEqualTo(new SocketAddress("0.0.0.0", 7687));
    }

    @Test
    void shouldDeriveBoltListenAddressFromDefaultListenAddressAndSpecifiedPort() {
        // given
        Config config = Config.newBuilder()
                .set(GraphDatabaseSettings.default_listen_address, new SocketAddress("0.0.0.0"))
                .set(BoltConnector.enabled, true)
                .set(BoltConnector.listen_address, new SocketAddress(8000))
                .build();

        // then
        assertThat(config.get(BoltConnector.listen_address)).isEqualTo(new SocketAddress("0.0.0.0", 8000));
    }

    @Test
    void testServerDefaultSettings() {
        // given
        Config config = Config.newBuilder()
                .setDefaults(GraphDatabaseSettings.SERVER_DEFAULTS)
                .build();

        // then
        assertThat(config.get(HttpConnector.listen_address)).isEqualTo(new SocketAddress("localhost", 7474));
        assertThat(config.get(HttpsConnector.listen_address)).isEqualTo(new SocketAddress("localhost", 7473));
        assertThat(config.get(BoltConnector.listen_address)).isEqualTo(new SocketAddress("localhost", 7687));

        assertThat(config.get(HttpConnector.enabled)).isTrue();
        assertThat(config.get(HttpsConnector.enabled)).isFalse();
        assertThat(config.get(BoltConnector.enabled)).isTrue();
    }

    @Test
    void hasDefaultBookmarkAwaitTimeout() {
        Config config = Config.defaults();
        long bookmarkReadyTimeoutMs =
                config.get(GraphDatabaseSettings.bookmark_ready_timeout).toMillis();
        assertThat(bookmarkReadyTimeoutMs).isEqualTo(TimeUnit.SECONDS.toMillis(30));
    }

    @Test
    void throwsForIllegalBookmarkAwaitTimeout() {
        String[] illegalValues = {"0ms", "0s", "10ms", "99ms", "999ms", "42ms"};

        for (String value : illegalValues) {
            assertThatThrownBy(() -> {
                        Config config =
                                Config.defaults(GraphDatabaseSettings.bookmark_ready_timeout, DURATION.parse(value));
                        config.get(GraphDatabaseSettings.bookmark_ready_timeout);
                    })
                    .as("Exception expected for value '" + value + "'")
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void shouldDeriveListenAddressFromDefaultListenAddress() {
        // given
        Config config = Config.newBuilder()
                .set(GraphDatabaseSettings.default_listen_address, new SocketAddress("0.0.0.0"))
                .setDefaults(GraphDatabaseSettings.SERVER_DEFAULTS)
                .build();

        // then
        assertThat(config.get(HttpConnector.listen_address).getHostname()).isEqualTo("0.0.0.0");
        assertThat(config.get(HttpsConnector.listen_address).getHostname()).isEqualTo("0.0.0.0");
        assertThat(config.get(BoltConnector.listen_address).getHostname()).isEqualTo("0.0.0.0");
    }

    @Test
    void shouldDeriveListenAddressFromDefaultListenAddressAndSpecifiedPorts() {
        // given
        Config config = Config.newBuilder()
                .set(GraphDatabaseSettings.default_listen_address, new SocketAddress("0.0.0.0"))
                .set(HttpConnector.listen_address, new SocketAddress(8000))
                .set(HttpsConnector.listen_address, new SocketAddress(9000))
                .set(BoltConnector.listen_address, new SocketAddress(10000))
                .build();

        // then
        assertThat(config.get(HttpsConnector.listen_address)).isEqualTo(new SocketAddress("0.0.0.0", 9000));
        assertThat(config.get(HttpConnector.listen_address)).isEqualTo(new SocketAddress("0.0.0.0", 8000));
        assertThat(config.get(BoltConnector.listen_address)).isEqualTo(new SocketAddress("0.0.0.0", 10000));
    }

    @Test
    void validateRetentionPolicy() {
        String[] validSet = new String[] {
            "true",
            "keep_all",
            "false",
            "keep_none",
            "10 files",
            "10k files",
            "10K size",
            "10m txs",
            "10M entries",
            "10g hours",
            "10G days"
        };

        String[] invalidSet = new String[] {"invalid", "all", "10", "10k", "10k a"};

        for (String valid : validSet) {
            assertThat(Config.defaults(keep_logical_logs, valid).get(keep_logical_logs))
                    .isEqualTo(valid);
        }

        for (String invalid : invalidSet) {
            assertThatThrownBy(() -> Config.defaults(keep_logical_logs, invalid))
                    .as("Value \"" + invalid + "\" should be considered invalid")
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void transactionSamplingCanBePercentageValues() {
        IntStream range = IntStream.range(1, 101);
        range.forEach(percentage -> {
            Config config = Config.defaults(transaction_sampling_percentage, percentage);
            int configuredSampling = config.get(transaction_sampling_percentage);
            assertThat(configuredSampling).isEqualTo(percentage);
        });
        assertThatThrownBy(() -> Config.defaults(transaction_sampling_percentage, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Config.defaults(transaction_sampling_percentage, 101))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Config.defaults(transaction_sampling_percentage, 10101))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validateTransactionTracingLevelValues() {
        GraphDatabaseSettings.TransactionTracingLevel[] values = GraphDatabaseSettings.TransactionTracingLevel.values();
        for (GraphDatabaseSettings.TransactionTracingLevel level : values) {
            assertThat(Config.defaults(transaction_tracing_level, level).get(transaction_tracing_level))
                    .isEqualTo(level);
        }
        assertThatThrownBy(() -> Config.newBuilder()
                        .setRaw(Map.of(transaction_tracing_level.name(), "TRACE"))
                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void configValuesContainsConnectors() {
        assertThat(GraphDatabaseSettings.SERVER_DEFAULTS.keySet().stream()
                        .map(Setting::name)
                        .collect(Collectors.toList()))
                .contains(
                        "server.http.enabled",
                        "server.https.enabled",
                        "server.bolt.enabled",
                        "dbms.security.auth_enabled");
    }

    @Test
    void testDefaultAddressOnlyAllowsHostname() {
        assertThatCode(() -> Config.defaults(default_listen_address, new SocketAddress("foo")))
                .doesNotThrowAnyException();
        assertThatCode(() -> Config.defaults(default_advertised_address, new SocketAddress("bar")))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> Config.defaults(default_listen_address, new SocketAddress("foo", 123)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Config.defaults(default_advertised_address, new SocketAddress("bar", 456)))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> Config.defaults(default_listen_address, new SocketAddress(123)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Config.defaults(default_advertised_address, new SocketAddress(456)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDefaultCypherVersion() throws IOException {
        for (GraphDatabaseSettings.CypherVersion cv : GraphDatabaseSettings.CypherVersion.values()) {
            Path cfg = directory.file("cfg");
            Files.writeString(
                    cfg,
                    default_language.name() + "=" + cv.toString(),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
            final var baseConfig = Config.newBuilder().fromFile(cfg).build();

            // All versions allowed
            assertThat(baseConfig.get(default_language)).isEqualTo(cv);
        }
    }

    @Test
    void rememberTestingWhenCypher25IsDefault() {
        assertThat(default_language.defaultValue()).as("""
                        You have set the default query language to Cypher 25.
                        Do not forget:
                        - Some nightly test builds runs with `-p default-query-lang-cypher-25`,
                          these should be updated to run on Cypher 5 now.
                          Remove usage of the CypherFunSuite.Tags.NoQueryLangOverride tag where it's not needed.
                          Remove usage of the org.neo4j.test.tags.NoQueryLangOverrideTag tag where it's not needed.
                        - Make sure org.neo4j.cypher.cucumber.glue.regular.TestConf looks good.
                          Configurations there probably assume the default is Cypher 5.
                        - Go though CypherFeatureTests.scala and make sure all versions are covered.
                        - Update the assertion of this test to pass, to be reminded the next time we update the default.
                        """).isEqualTo(GraphDatabaseSettings.CypherVersion.Cypher5);
    }
}
