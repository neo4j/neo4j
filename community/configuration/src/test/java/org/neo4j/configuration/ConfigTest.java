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
import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GroupSettingHelper.getBuilder;
import static org.neo4j.configuration.SettingConstraints.dependency;
import static org.neo4j.configuration.SettingConstraints.is;
import static org.neo4j.configuration.SettingConstraints.max;
import static org.neo4j.configuration.SettingConstraints.unconstrained;
import static org.neo4j.configuration.SettingConstraints.valueDependency;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config.ValueSource;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLog;
import org.neo4j.test.extension.DisabledForRoot;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.util.FeatureToggles;

@TestDirectoryExtension
class ConfigTest {
    private static final Set<PosixFilePermission> permittedFilePermissionsForCommandExpansion =
            Set.of(OWNER_READ, OWNER_WRITE, GROUP_READ);
    private static final Set<PosixFilePermission> forbiddenFilePermissionsForCommandExpansion =
            Set.of(OWNER_EXECUTE, GROUP_WRITE, GROUP_EXECUTE, OTHERS_READ, OTHERS_WRITE, OTHERS_EXECUTE);

    @Inject
    private TestDirectory testDirectory;

    @Test
    void testLoadSettingsToConfig() {
        Config config = Config.newBuilder().addSettingsClass(TestSettings.class).build();
        assertThat(config.get(TestSettings.stringSetting)).isEqualTo("hello");
        assertThat(config.get(TestSettings.intSetting)).isEqualTo(1);
        assertThat(config.get(TestSettings.intListSetting)).containsExactly(1);
        assertThat(config.get(TestSettings.boolSetting)).isNull();
    }

    @Test
    void failToBuildConfigForSettingInWrongNamespace() {
        assertThatThrownBy(() -> Config.newBuilder()
                        .addSettingsClass(WrongNamespaceSettings.class)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Setting: 'planet.express.open' name does not reside in any of the supported setting namespaces which are: dbms., db., browser., server., internal.");
    }

    @Test
    void buildConfigForSettingInWrongNamespaceWhenStrictDisabled() {
        assertThatCode(() -> Config.newBuilder()
                        .addSettingsClass(WrongNamespaceSettings.class)
                        .set(GraphDatabaseSettings.strict_config_validation, false)
                        .build())
                .doesNotThrowAnyException();
    }

    @Test
    void failToBuildConfigForInternalSettingInWrongNamespace() {
        assertThatThrownBy(() -> Config.newBuilder()
                        .addSettingsClass(InternalWrongNamespaceSettings.class)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Setting: 'server.setting.not_really.internal' is internal but does not reside in the correct internal settings namespace.");
    }

    @Test
    void failToBuildConfigForPublicSettingInInternalNamespace() {
        assertThatThrownBy(() -> Config.newBuilder()
                        .addSettingsClass(PublicWrongNamespaceSettings.class)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Setting: 'setting.not_really.internal' is not internal but using internal settings namespace.");
    }

    @Test
    void failToBuildConfigForPublicSettingInLegacyUnsupportedNamespace() {
        assertThatThrownBy(() -> Config.newBuilder()
                        .addSettingsClass(LegacyUnsupportedNamespaceSettings.class)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        " Setting: 'setting.unsupported_or_not_really' is not internal but using internal settings namespace.");
    }

    @Test
    void testFetchAbsentSetting() {
        Config config = Config.newBuilder().addSettingsClass(TestSettings.class).build();
        Setting<Boolean> absentSetting =
                newBuilder("test.absent.bool", BOOL, null).build();
        assertThatThrownBy(() -> config.get(absentSetting)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testUpdateValue() {
        Config config = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .set(TestSettings.intSetting, 3)
                .build();
        assertThat(config.get(TestSettings.intSetting)).isEqualTo(3);
        config.setDynamic(TestSettings.intSetting, 2, getClass().getSimpleName());
        assertThat(config.get(TestSettings.intSetting)).isEqualTo(2);
        config.setDynamic(TestSettings.intSetting, null, getClass().getSimpleName());
        assertThat(config.get(TestSettings.intSetting)).isEqualTo(1);
    }

    @Test
    void testSetConstrainedValue() {
        Config.Builder builder =
                Config.newBuilder().addSettingsClass(TestSettings.class).set(TestSettings.constrainedIntSetting, 4);
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);
        builder.set(TestSettings.constrainedIntSetting, 2);
        assertThatCode(builder::build).doesNotThrowAnyException();
    }

    @Test
    void testUpdateConstrainedValue() {
        Config config = Config.newBuilder().addSettingsClass(TestSettings.class).build();
        assertThatThrownBy(() -> config.setDynamic(
                        TestSettings.constrainedIntSetting, 4, getClass().getSimpleName()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(config.get(TestSettings.constrainedIntSetting)).isEqualTo(1);
        assertThatCode(() -> config.setDynamic(
                        TestSettings.constrainedIntSetting, 2, getClass().getSimpleName()))
                .doesNotThrowAnyException();
    }

    @Test
    void testOverrideAbsentSetting() {
        Map<String, String> settings = Map.of("test.absent.bool", FALSE);
        Config.Builder builder = Config.newBuilder()
                .set(GraphDatabaseSettings.strict_config_validation, true)
                .addSettingsClass(TestSettings.class)
                .setRaw(settings);
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testOverrideDefault() {

        Map<Setting<?>, Object> overriddenDefaults =
                Map.of(TestSettings.stringSetting, "foo", TestSettings.intSetting, 11, TestSettings.boolSetting, true);

        Config config = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .setDefaults(overriddenDefaults)
                .build();

        assertThat(config.get(TestSettings.stringSetting)).isEqualTo("foo");
        assertThat(config.get(TestSettings.intSetting)).isEqualTo(11);
        assertThat(config.get(TestSettings.boolSetting)).isEqualTo(true);
    }

    @Test
    void testUpdateStatic() {
        Config config = Config.newBuilder().addSettingsClass(TestSettings.class).build();
        assertThatThrownBy(() -> config.setDynamic(
                        TestSettings.stringSetting, "not allowed", getClass().getSimpleName()))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(config.get(TestSettings.stringSetting)).isEqualTo("hello");
        config.set(TestSettings.stringSetting, "allowed internally");
        assertThat(config.get(TestSettings.stringSetting)).isEqualTo("allowed internally");
    }

    @Test
    void testUpdateImmutable() {
        Config config = Config.newBuilder().addSettingsClass(TestSettings.class).build();
        assertThatThrownBy(() -> config.setDynamic(
                        TestSettings.boolSetting, true, getClass().getSimpleName()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> config.set(TestSettings.boolSetting, true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testObserver() {
        Config config = Config.newBuilder().addSettingsClass(TestSettings.class).build();

        MutableInt observedOld = new MutableInt(0);
        MutableInt observedNew = new MutableInt(0);
        SettingChangeListener<Integer> listener = (oldValue, newValue) -> {
            observedOld.setValue(oldValue);
            observedNew.setValue(newValue);
        };

        config.addListener(TestSettings.intSetting, listener);

        assertThat(observedOld.getValue()).isEqualTo(0);
        assertThat(observedNew.getValue()).isEqualTo(0);

        config.setDynamic(TestSettings.intSetting, 2, getClass().getSimpleName());
        assertThat(observedOld.getValue()).isEqualTo(1);
        assertThat(observedNew.getValue()).isEqualTo(2);

        config.setDynamic(TestSettings.intSetting, 7, getClass().getSimpleName());
        assertThat(observedOld.getValue()).isEqualTo(2);
        assertThat(observedNew.getValue()).isEqualTo(7);

        config.removeListener(TestSettings.intSetting, listener);

        config.setDynamic(TestSettings.intSetting, 9, getClass().getSimpleName());
        assertThat(observedOld.getValue()).isEqualTo(2);
        assertThat(observedNew.getValue()).isEqualTo(7);

        assertThatThrownBy(() -> config.addListener(TestSettings.boolSetting, (oV, nV) -> {}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGroup() {
        var g1 = TestConnectionGroupSetting.group("1");
        var g2 = TestConnectionGroupSetting.group("2");
        Config config = Config.newBuilder()
                .addGroupSettingClass(TestConnectionGroupSetting.class)
                .set(g1.port, 1111)
                .set(g1.hostname, "0.0.0.0")
                .set(g1.secure, false)
                .set(g2.port, 2222)
                .set(g2.hostname, "127.0.0.1")
                .build();

        assertThat(config.get(g1.port)).isEqualTo(1111);
        assertThat(config.get(g2.port)).isEqualTo(2222);
        assertThat(config.get(g1.secure)).isEqualTo(false);
        assertThat(config.get(g2.secure)).isEqualTo(true);

        assertThatThrownBy(() -> config.get(TestConnectionGroupSetting.group("not_specified_id").port))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDynamicGroup() {
        var g1 = TestDynamicGroupSetting.group("1");
        var g2 = TestDynamicGroupSetting.group("2");
        Config config = Config.newBuilder()
                .addGroupSettingClass(TestDynamicGroupSetting.class)
                .set(g1.value, "value1")
                .set(g2.value, "value2")
                .build();

        assertThat(config.get(g1.value)).isEqualTo("value1");
        assertThat(config.get(g2.value)).isEqualTo("value2");

        config.setDynamic(g1.value, "new1", getClass().getSimpleName());
        assertThat(config.get(g1.value)).isEqualTo("new1");
        assertThat(config.get(g2.value)).isEqualTo("value2");

        config.setDynamic(g2.value, "new2", getClass().getSimpleName());
        assertThat(config.get(g2.value)).isEqualTo("new2");

        var groups = config.getGroups(TestDynamicGroupSetting.class);
        assertThat(groups).hasSize(2);
        assertThat(config.get(groups.get("1").value)).isEqualTo("new1");
        assertThat(config.get(groups.get("2").value)).isEqualTo("new2");
    }

    @Test
    void testDynamicGroupWithConstraint() {
        var g1 = TestDynamicGroupSetting.group("1");
        var g2 = TestDynamicGroupSetting.group("2");
        Config config = Config.newBuilder()
                .addGroupSettingClass(TestDynamicGroupSetting.class)
                .set(g1.constrainedValue, "avalue1")
                .set(g2.value, "value")
                .build();

        assertThat(config.get(g1.constrainedValue)).isEqualTo("avalue1");
        assertThat(config.get(g2.constrainedValue)).isEqualTo("aDefaultValue");

        config.setDynamic(g1.constrainedValue, "aNewValue", getClass().getSimpleName());
        assertThat(config.get(g1.constrainedValue)).isEqualTo("aNewValue");
        assertThat(config.get(g2.constrainedValue)).isEqualTo("aDefaultValue");

        assertThatThrownBy(() -> config.setDynamic(
                        g2.constrainedValue, "new2", getClass().getSimpleName()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(config.get(g2.constrainedValue)).isEqualTo("aDefaultValue");
        assertThat(config.get(g1.constrainedValue)).isEqualTo("aNewValue");

        var groups = config.getGroups(TestDynamicGroupSetting.class);
        assertThat(groups).hasSize(2);
        assertThat(config.get(groups.get("1").constrainedValue)).isEqualTo("aNewValue");
        assertThat(config.get(groups.get("2").constrainedValue)).isEqualTo("aDefaultValue");
    }

    @Test
    void testDynamicGroupFromConfigs() {
        var g1 = TestDynamicGroupSetting.group("1");
        var g2 = TestDynamicGroupSetting.group("2");
        Config config1 = Config.newBuilder()
                .addGroupSettingClass(TestDynamicGroupSetting.class)
                .set(TestDynamicGroupSetting.group("1").value, "value1")
                .set(TestDynamicGroupSetting.group("2").value, "value2")
                .build();

        assertThat(config1.get(g1.value)).isEqualTo("value1");
        assertThat(config1.get(g2.value)).isEqualTo("value2");

        Config config2 = Config.newBuilder()
                .addGroupSettingClass(TestDynamicGroupSetting.class)
                .set(TestDynamicGroupSetting.group("1").value, "value1")
                .set(TestDynamicGroupSetting.group("2").value, "value2")
                .build();

        assertThat(config2.get(g1.value)).isEqualTo("value1");
        assertThat(config2.get(g2.value)).isEqualTo("value2");

        config1.setDynamic(
                TestDynamicGroupSetting.group("1").value, "new1", getClass().getSimpleName());
        config1.setDynamic(
                TestDynamicGroupSetting.group("2").value, "new2", getClass().getSimpleName());

        var groups1 = config1.getGroups(TestDynamicGroupSetting.class);
        assertThat(groups1).hasSize(2);
        assertThat(config1.get(groups1.get("1").value)).isEqualTo("new1");
        assertThat(config1.get(groups1.get("2").value)).isEqualTo("new2");

        var groups2 = config2.getGroups(TestDynamicGroupSetting.class);
        assertThat(groups2).hasSize(2);
        assertThat(config2.get(groups2.get("1").value)).isEqualTo("value1");
        assertThat(config2.get(groups2.get("2").value)).isEqualTo("value2");
    }

    @Test
    void testDynamicGroupObserver() {
        var g1 = TestDynamicGroupSetting.group("1");
        var g2 = TestDynamicGroupSetting.group("2");

        Config config = Config.newBuilder()
                .addGroupSettingClass(TestDynamicGroupSetting.class)
                .set(g1.value, "value1")
                .set(g2.value, "value2")
                .build();

        config.addListener(g1.value, (oldValue, newValue) -> {
            assertThat(oldValue).isEqualTo("value1");
            assertThat(newValue).isEqualTo("new1");
        });
        config.addListener(g2.value, (oldValue, newValue) -> {
            assertThat(oldValue).isEqualTo("value2");
            assertThat(newValue).isEqualTo("new2");
        });

        config.setDynamic(g1.value, "new1", getClass().getSimpleName());
        assertThat(config.get(g1.value)).isEqualTo("new1");
        assertThat(config.get(g2.value)).isEqualTo("value2");

        config.setDynamic(g2.value, "new2", getClass().getSimpleName());
        assertThat(config.get(g2.value)).isEqualTo("new2");

        var groups = config.getGroups(TestDynamicGroupSetting.class);
        assertThat(groups).hasSize(2);
        assertThat(config.get(groups.get("1").value)).isEqualTo("new1");
        assertThat(config.get(groups.get("2").value)).isEqualTo("new2");
    }

    @Test
    void testGroupInheritance() {
        ChildGroup group = new ChildGroup("1");
        Config config = Config.newBuilder()
                .addGroupSettingClass(ChildGroup.class)
                .set(group.childSetting, "child")
                .build();

        assertThat(config.get(group.childSetting)).isEqualTo("child");
        assertThat(config.get(group.parentSetting)).isEqualTo("parent");
    }

    @Test
    void testDynamicGroupInheritance() {
        ChildDynamicGroup group1 = new ChildDynamicGroup("1");
        ChildDynamicGroup group2 = new ChildDynamicGroup("2");
        Config config = Config.newBuilder()
                .addGroupSettingClass(ChildDynamicGroup.class)
                .set(group1.childSetting, "child")
                .set(group2.childSetting, "child")
                .build();

        config.setDynamic(group1.parentSetting, "newParent", getClass().getSimpleName());
        assertThat(config.get(group1.parentSetting)).isEqualTo("newParent");
        assertThat(config.get(group2.parentSetting)).isEqualTo("parent");

        config.setDynamic(group1.childSetting, "newChild", getClass().getSimpleName());
        assertThat(config.get(group1.childSetting)).isEqualTo("newChild");
        assertThat(config.get(group2.childSetting)).isEqualTo("child");

        assertThat(config.get(config.getGroups(ChildDynamicGroup.class).get("1").childSetting))
                .isEqualTo("newChild");
        assertThat(config.get(config.getGroups(ChildDynamicGroup.class).get("1").parentSetting))
                .isEqualTo("newParent");

        assertThat(config.get(config.getGroups(ChildDynamicGroup.class).get("2").childSetting))
                .isEqualTo("child");
        assertThat(config.get(config.getGroups(ChildDynamicGroup.class).get("2").parentSetting))
                .isEqualTo("parent");
    }

    @Test
    void testMalformedGroupSetting() {
        Map<String, String> settings = Map.of("dbms.test.connection.http..foo.bar", "1111");

        Config.Builder builder = Config.newBuilder()
                .set(GraphDatabaseSettings.strict_config_validation, true)
                .addGroupSettingClass(TestConnectionGroupSetting.class)
                .setRaw(settings);

        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGetGroups() {
        Config config = Config.newBuilder()
                .addGroupSettingClass(TestConnectionGroupSetting.class)
                .set(TestConnectionGroupSetting.group("default").port, 7474)
                .set(TestConnectionGroupSetting.group("1").port, 1111)
                .set(TestConnectionGroupSetting.group("1").hostname, "0.0.0.0")
                .set(TestConnectionGroupSetting.group("1").secure, false)
                .set(TestConnectionGroupSetting.group("2").port, 2222)
                .set(TestConnectionGroupSetting.group("2").hostname, "127.0.0.1")
                .build();

        var groups = config.getGroups(TestConnectionGroupSetting.class);
        assertThat(groups.keySet()).isEqualTo(Set.of("default", "1", "2"));
        assertThat(config.get(groups.get("default").port)).isEqualTo(7474);
        assertThat(config.get(groups.get("2").secure)).isTrue();
    }

    @Test
    void testFromConfig() {
        Config fromConfig = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .setDefault(TestSettings.boolSetting, false)
                .set(TestSettings.intSetting, 3)
                .build();

        Config config1 = Config.newBuilder().fromConfig(fromConfig).build();
        assertThat(config1.get(TestSettings.intSetting)).isEqualTo(3);
        assertThat(config1.get(TestSettings.stringSetting)).isEqualTo("hello");

        Config config2 = Config.newBuilder()
                .fromConfig(fromConfig)
                .set(TestSettings.intSetting, 5)
                .build();

        assertThat(config2.get(TestSettings.intSetting)).isEqualTo(5);

        Config config3 = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .fromConfig(fromConfig)
                .set(TestSettings.intSetting, 7)
                .build();

        assertThat(config3.get(TestSettings.intSetting)).isEqualTo(7);
        assertThat(config3.get(TestSettings.boolSetting)).isFalse();
    }

    @Test
    void shouldThrowIfMultipleFromConfig() {
        Config fromConfig = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .setDefault(TestSettings.boolSetting, false)
                .set(TestSettings.intSetting, 3)
                .build();

        assertThatThrownBy(() -> Config.newBuilder()
                        .fromConfig(fromConfig)
                        .fromConfig(fromConfig)
                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGroupFromConfig() {
        Config fromConfig = Config.newBuilder()
                .addGroupSettingClass(TestConnectionGroupSetting.class)
                .set(TestConnectionGroupSetting.group("default").port, 7474)
                .set(TestConnectionGroupSetting.group("1").port, 1111)
                .set(TestConnectionGroupSetting.group("1").hostname, "0.0.0.0")
                .set(TestConnectionGroupSetting.group("1").secure, false)
                .build();

        Config config1 = Config.newBuilder().fromConfig(fromConfig).build();

        var groups1 = config1.getGroups(TestConnectionGroupSetting.class);
        assertThat(groups1.keySet()).isEqualTo(Set.of("default", "1"));
        assertThat(config1.get(groups1.get("default").port)).isEqualTo(7474);

        Config config2 = Config.newBuilder()
                .fromConfig(fromConfig)
                .addGroupSettingClass(TestConnectionGroupSetting.class)
                .set(TestConnectionGroupSetting.group("1").port, 3333)
                .set(TestConnectionGroupSetting.group("2").port, 2222)
                .set(TestConnectionGroupSetting.group("2").hostname, "127.0.0.1")
                .build();

        var groups2 = config2.getGroups(TestConnectionGroupSetting.class);
        assertThat(groups2.keySet()).isEqualTo(Set.of("default", "1", "2"));
        assertThat(config2.get(groups2.get("default").port)).isEqualTo(7474);
        assertThat(config2.get(groups2.get("1").port)).isEqualTo(3333);
        assertThat(config2.get(groups2.get("default").secure)).isTrue();
        assertThat(config2.get(groups2.get("2").secure)).isTrue();
    }

    @Test
    void testResolveDefaultSettingDependency() {
        Config.Builder builder = Config.newBuilder().addSettingsClass(DependencySettings.class);

        {
            Config config = builder.build();
            assertThat(config.get(DependencySettings.dependingString))
                    .isEqualTo(config.get(DependencySettings.baseString));
        }
        {
            String value = "default overrides dependency";
            builder.setDefault(DependencySettings.dependingString, value);
            Config config = builder.build();
            assertThat(config.get(DependencySettings.dependingString)).isEqualTo(value);
        }

        {
            String value = "value overrides dependency";
            builder.set(DependencySettings.dependingString, value);
            Config config = builder.build();
            assertThat(config.get(DependencySettings.dependingString)).isEqualTo(value);
        }
    }

    @Test
    void testResolvePathSettingDependency() {
        Config config =
                Config.newBuilder().addSettingsClass(DependencySettings.class).build();

        assertThat(config.get(DependencySettings.basePath))
                .isEqualTo(Path.of("/base/").toAbsolutePath());
        assertThat(config.get(DependencySettings.midPath))
                .isEqualTo(Path.of("/base/mid/").toAbsolutePath());
        assertThat(config.get(DependencySettings.endPath))
                .isEqualTo(Path.of("/base/mid/end/file").toAbsolutePath());
        assertThat(config.get(DependencySettings.absolute))
                .isEqualTo(Path.of("/another/path/file").toAbsolutePath());

        config.set(DependencySettings.endPath, Path.of("/path/another_file"));
        config.set(DependencySettings.absolute, Path.of("path/another_file"));
        assertThat(config.get(DependencySettings.endPath))
                .isEqualTo(Path.of("/path/another_file").toAbsolutePath());
        assertThat(config.get(DependencySettings.absolute))
                .isEqualTo(Path.of("/base/mid/path/another_file").toAbsolutePath());
    }

    private static final class BrokenDependencySettings implements SettingsDeclaration {
        static final Setting<Path> broken = newBuilder("test.base.path", PATH, Path.of("/base/"))
                .setDependency(newBuilder("test.not.present.dependency", PATH, Path.of("/broken/"))
                        .immutable()
                        .build())
                .immutable()
                .build();
    }

    @Test
    void testResolveBrokenSettingDependency() {
        Config.Builder builder = Config.newBuilder().addSettingsClass(BrokenDependencySettings.class);
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);
    }

    private static final class SingleSettingGroup implements GroupSetting {
        final Setting<String> singleSetting;
        private final String name;

        static SingleSettingGroup group(String name) {
            return new SingleSettingGroup(name);
        }

        private SingleSettingGroup(String name) {
            this.name = name;
            singleSetting = getBuilder(getPrefix(), name, STRING, null).build();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String getPrefix() {
            return "db.test.single_setting";
        }
    }

    @Test
    void testSingleSettingGroup() {
        Map<String, String> fromSettings = Map.of(
                "db.test.single_setting.default", "default value",
                "db.test.single_setting.foo", "foo",
                "db.test.single_setting.bar", "bar");
        Config config = Config.newBuilder()
                .addGroupSettingClass(SingleSettingGroup.class)
                .setRaw(fromSettings)
                .build();

        assertThat(config.getGroups(SingleSettingGroup.class)).hasSize(3);
        assertThat(config.get(SingleSettingGroup.group("default").singleSetting))
                .isEqualTo("default value");
        assertThat(config.get(SingleSettingGroup.group("foo").singleSetting)).isEqualTo("foo");
        assertThat(config.get(SingleSettingGroup.group("bar").singleSetting)).isEqualTo("bar");
    }

    @Test
    void shouldLogIfConfigFileCouldNotBeFound() {
        InternalLog log = mock(InternalLog.class);
        Path confFile = testDirectory.file("test.conf"); // Note: we don't create the file.

        Config config = Config.emptyBuilder().fromFileNoThrow(confFile).build();

        config.setLogger(log);

        verify(log).warn("Config file [%s] does not exist.", confFile);
    }

    @Test
    @DisabledForRoot
    void shouldLogIfConfigFileCouldNotBeRead() throws IOException {
        AssertableLogProvider logProvider = new AssertableLogProvider(true);
        InternalLog log = logProvider.getLog(Config.class);
        Path confFile = testDirectory.file("test.conf");
        assertThat(confFile.toFile().createNewFile()).isTrue();
        assumeTrue(confFile.toFile().setReadable(false));

        Config config = Config.emptyBuilder().fromFileNoThrow(confFile).build();
        config.setLogger(log);

        assertThat(logProvider).containsMessages("Unable to load config file [%s]");
    }

    @Test
    void canReadConfigFile() throws IOException {
        Path confFile = testDirectory.file("test.conf");
        Files.write(
                confFile, Collections.singletonList(GraphDatabaseSettings.initial_default_database.name() + "=foo"));

        Config config1 = buildWithoutErrorsOrWarnings(Config.newBuilder().fromFile(confFile)::build);
        Config config2 = buildWithoutErrorsOrWarnings(Config.newBuilder().fromFileNoThrow(confFile)::build);
        assertThat(List.of(config1, config2))
                .extracting(c -> c.get(GraphDatabaseSettings.initial_default_database))
                .containsOnly("foo");
    }

    @Test
    void canOverrideDefaultCharset() throws IOException {
        final var unicodeString = "åäö\u1234";
        final var latin1String = "Ã¥Ã¤Ã¶á\u0088´";

        Path confFile = testDirectory.file("test.conf");
        // Writes UTF-8
        Files.write(
                confFile,
                Collections.singletonList(GraphDatabaseSettings.procedure_allowlist.name() + "=" + unicodeString));

        // Try reading with default charset (ISO 8859-1)
        Config config1 = buildWithoutErrorsOrWarnings(
                Config.newBuilder().setFileCharset(StandardCharsets.ISO_8859_1).fromFile(confFile)::build);
        assertThat(config1.get(GraphDatabaseSettings.procedure_allowlist)).containsExactly(latin1String);

        // Try reading with UTF-8
        Config config2 = buildWithoutErrorsOrWarnings(Config.newBuilder().fromFile(confFile)::build);
        assertThat(config2.get(GraphDatabaseSettings.procedure_allowlist)).containsExactly(unicodeString);
    }

    @Test
    void canReadEscapedCharsInPathsUnescapedFromFile() throws IOException {
        Path confFile = testDirectory.file("test.conf");
        Files.writeString(confFile, GraphDatabaseSettings.data_directory.name() + "=\\test\folder");

        Config conf = buildWithoutErrorsOrWarnings(Config.newBuilder().fromFile(confFile)::build);
        assertThat(conf.get(GraphDatabaseSettings.data_directory).toAbsolutePath())
                .isEqualTo(Path.of("/test/folder").toAbsolutePath());
    }

    @Test
    void canReadConfigDir() throws IOException {
        Path confDir = testDirectory.directory("test.conf");
        Path defaultDatabase = confDir.resolve(GraphDatabaseSettings.initial_default_database.name());
        Files.write(defaultDatabase, "foo".getBytes());

        Config config1 = buildWithoutErrorsOrWarnings(Config.newBuilder().fromFile(confDir)::build);
        Config config2 = buildWithoutErrorsOrWarnings(Config.newBuilder().fromFileNoThrow(confDir)::build);
        assertThat(List.of(config1, config2))
                .extracting(c -> c.get(GraphDatabaseSettings.initial_default_database))
                .containsOnly("foo");
    }

    @Test
    void ignoreSubdirsInConfigDir() throws IOException {
        Path confDir = testDirectory.directory("test.conf");
        Path subDir = Files.createDirectory(confDir.resolve("more"));

        Path defaultDatabase = subDir.resolve(GraphDatabaseSettings.initial_default_database.name());
        Files.write(defaultDatabase, "foo".getBytes());

        Config config1 = Config.newBuilder().fromFile(confDir).build();
        Config config2 = Config.newBuilder().fromFileNoThrow(confDir).build();

        Stream.of(config1, config2).forEach(c -> {
            AssertableLogProvider logProvider = new AssertableLogProvider();
            c.setLogger(logProvider.getLog(Config.class));
            assertThat(logProvider)
                    .forLevel(AssertableLogProvider.Level.WARN)
                    .containsMessages("Ignoring subdirectory in config directory [" + subDir + "].");
            assertThat(logProvider).forLevel(AssertableLogProvider.Level.ERROR).doesNotHaveAnyLogs();

            assertThat(c.get(GraphDatabaseSettings.initial_default_database)).isNotEqualTo("foo");
        });
    }

    /**
     * This test is supposed to run and pass on Windows *and* Linux/Mac
     * @throws IOException
     */
    @Test
    void canReadK8sStyleConfigDir() throws IOException {
        Path confDir = createK8sStyleConfigDir(Set.of());

        Config config = buildWithoutErrorsOrWarnings(Config.newBuilder().fromFile(confDir)::build);
        Config config2 = buildWithoutErrorsOrWarnings(Config.newBuilder().fromFileNoThrow(confDir)::build);

        assertThat(List.of(config, config2)).allSatisfy(c -> {
            assertThat(c.get(GraphDatabaseSettings.initial_default_database)).isEqualTo("foo");
            assertThat(c.get(GraphDatabaseSettings.auth_enabled)).isTrue();
            assertThat(c.get(GraphDatabaseSettings.auth_max_failed_attempts)).isEqualTo(4);
        });
    }

    /**
     * Creates a configuration directory in the style of a Kubernetes ConfigMap mounted as a volume.
     *
     * This replicates of the unusual arrangements with links and metadata files/directories that can exist in Kubernetes mounted volumes.
     * If running on Windows the stuff about file permissions is ignored.
     *
     * @param posixFilePermissions file permissions to set on files in the config directory. This can be empty if command explansion is not being used.
     * @throws IOException
     */
    private Path createK8sStyleConfigDir(Set<PosixFilePermission> posixFilePermissions) throws IOException {
        // Create and populate a directory for files and directories that we will target using links
        Path targetDir = testDirectory.directory("links");

        Path dotFile = Files.createFile(targetDir.resolve("..data"));
        Path dotDir = Files.createDirectory(targetDir.resolve("..metadata"));

        Path defaultDatabase = targetDir.resolve(GraphDatabaseSettings.initial_default_database.name());
        Files.createFile(defaultDatabase);
        Files.write(defaultDatabase, "foo".getBytes());

        Path authEnabled = targetDir.resolve(GraphDatabaseSettings.auth_enabled.name());
        Files.createFile(authEnabled);
        Files.write(authEnabled, "true".getBytes());

        // Create and populate the actual conf dir
        Path confDir = testDirectory.directory("neo4j.conf");

        // -- Set up all the links --
        // Symbolic link to a dot file
        Files.createSymbolicLink(confDir.resolve(dotFile.getFileName()), dotFile);
        // Symbolic link to a dot directory
        Files.createSymbolicLink(confDir.resolve(dotDir.getFileName()), dotDir);
        // Symbolic link to an actual setting file we want read
        Files.createSymbolicLink(confDir.resolve(defaultDatabase.getFileName()), defaultDatabase);
        // Hard link to an actual setting file we want read
        Files.createLink(confDir.resolve(authEnabled.getFileName()), authEnabled);

        // -- Set up regular files/dirs in the conf dir --
        // A dot file (this one doesn't show up on K8s, but better safe than sorry)
        Files.createFile(confDir.resolve(".DS_STORE"));
        // A dot dir (this one doesn't show up on K8s, but better safe than sorry)
        Files.createDirectory(confDir.resolve("..version"));
        // An actual settings file we want to read
        Path authMaxFailedAttempts = confDir.resolve(GraphDatabaseSettings.auth_max_failed_attempts.name());
        Files.createFile(authMaxFailedAttempts);
        Files.write(authMaxFailedAttempts, "4".getBytes());

        if (!IS_OS_WINDOWS && !posixFilePermissions.isEmpty()) {
            setPosixFilePermissions(defaultDatabase, posixFilePermissions);
            setPosixFilePermissions(authEnabled, posixFilePermissions);
            setPosixFilePermissions(authMaxFailedAttempts, posixFilePermissions);
        }
        return confDir;
    }

    private static Config buildWithoutErrorsOrWarnings(Supplier<Config> buildConfig) {
        AssertableLogProvider lp = new AssertableLogProvider();

        Config config = buildConfig.get();

        // The config uses a buffering log, when you supply it with a log (i.e. our mock) it replays the buffered log
        // into it
        config.setLogger(lp.getLog(Config.class));
        assertThat(lp).forLevel(AssertableLogProvider.Level.WARN).doesNotHaveAnyLogs();
        assertThat(lp).forLevel(AssertableLogProvider.Level.ERROR).doesNotHaveAnyLogs();

        return config;
    }

    @Test
    void mustThrowIfConfigFileCouldNotBeFound() {
        assertThatThrownBy(() -> {
                    Path confFile = testDirectory.file("test.conf");
                    Config.emptyBuilder().fromFile(confFile).build();
                })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisabledForRoot
    void mustThrowIfConfigFileCouldNotBeRead() throws IOException {
        Path confFile = testDirectory.file("test.conf");
        assertThat(confFile.toFile().createNewFile()).isTrue();
        assumeTrue(confFile.toFile().setReadable(false));
        assertThatThrownBy(() -> Config.emptyBuilder().fromFile(confFile).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void mustWarnIfFileContainsDuplicateSettings() throws Exception {
        InternalLog log = mock(InternalLog.class);
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                Arrays.asList(
                        BootloaderSettings.initial_heap_size.name() + "=5g",
                        BootloaderSettings.initial_heap_size.name() + "=4g",
                        BootloaderSettings.initial_heap_size.name() + "=3g",
                        BootloaderSettings.max_heap_size.name() + "=10g",
                        BootloaderSettings.max_heap_size.name() + "=11g"));

        Config.Builder builder = Config.newBuilder()
                .set(GraphDatabaseSettings.strict_config_validation, false)
                .fromFile(confFile)
                .setDefault(BootloaderSettings.initial_heap_size, ByteUnit.gibiBytes(1))
                .setDefault(BootloaderSettings.initial_heap_size, ByteUnit.gibiBytes(2));

        Config config = builder.build();
        config.setLogger(log);

        // We should only log the warning once for each.
        verify(log)
                .warn(
                        "The '%s' setting is overridden. Setting value changed from '%s' to '%s'.",
                        BootloaderSettings.initial_heap_size.name(), "5g", "4g");
        verify(log)
                .warn(
                        "The '%s' setting is overridden. Setting value changed from '%s' to '%s'.",
                        BootloaderSettings.initial_heap_size.name(), "4g", "3g");
        verify(log)
                .warn(
                        "The '%s' setting is overridden. Setting value changed from '%s' to '%s'.",
                        BootloaderSettings.max_heap_size.name(), "10g", "11g");

        builder.set(GraphDatabaseSettings.strict_config_validation, true);
        assertThatThrownBy(builder::build).hasMessageContaining("declared multiple times");
    }

    @Test
    void testDisableAllConnectors() {
        Config config = Config.newBuilder()
                .set(BoltConnector.enabled, true)
                .set(HttpConnector.enabled, true)
                .set(HttpsConnector.enabled, true)
                .build();

        ConfigUtils.disableAllConnectors(config);

        assertThat(config.get(BoltConnector.enabled)).isFalse();
        assertThat(config.get(HttpConnector.enabled)).isFalse();
        assertThat(config.get(HttpsConnector.enabled)).isFalse();
    }

    @Test
    void testAmendIfNotSet() {
        Config config = Config.newBuilder().addSettingsClass(TestSettings.class).build();
        config.setIfNotSet(TestSettings.intSetting, 77);
        assertThat(config.get(TestSettings.intSetting)).isEqualTo(77);

        Config configWithSetting = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .set(TestSettings.intSetting, 66)
                .build();
        configWithSetting.setIfNotSet(TestSettings.intSetting, 77);
        assertThat(configWithSetting.get(TestSettings.intSetting)).isEqualTo(66);
    }

    @Test
    void testIsExplicitlySet() {
        Config config =
                Config.emptyBuilder().addSettingsClass(TestSettings.class).build();
        assertThat(config.isExplicitlySet(TestSettings.intSetting)).isFalse();
        config.set(TestSettings.intSetting, 77);
        assertThat(config.isExplicitlySet(TestSettings.intSetting)).isTrue();

        Config configWithSetting = Config.emptyBuilder()
                .addSettingsClass(TestSettings.class)
                .set(TestSettings.intSetting, 66)
                .build();
        assertThat(configWithSetting.isExplicitlySet(TestSettings.intSetting)).isTrue();
        configWithSetting.set(TestSettings.intSetting, null);
        assertThat(configWithSetting.isExplicitlySet(TestSettings.intSetting)).isFalse();
    }

    @Test
    void testStrictValidationForGarbage() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, Collections.singletonList("some_unrecognized_garbage=true"));

        Config.Builder builder = Config.newBuilder().fromFile(confFile);
        builder.set(GraphDatabaseSettings.strict_config_validation, true);
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);

        builder.set(GraphDatabaseSettings.strict_config_validation, false);
        assertThatCode(builder::build).doesNotThrowAnyException();
    }

    @Test
    void testStrictValidationForDuplicates() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        GraphDatabaseSettings.initial_default_database.name() + "=foo",
                        GraphDatabaseSettings.initial_default_database.name() + "=bar"));
        Config.Builder builder = Config.newBuilder().fromFile(confFile);
        builder.set(GraphDatabaseSettings.strict_config_validation, true);
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);

        builder.set(GraphDatabaseSettings.strict_config_validation, false);
        assertThatCode(builder::build).doesNotThrowAnyException();
    }

    @Test
    void testStrictValidationForGarbageAllowDuplicates() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, Collections.singletonList("some_unrecognized_garbage=true"));

        Config.Builder builder = Config.newBuilder().fromFile(confFile);
        builder.set(GraphDatabaseSettings.strict_config_validation, true);
        builder.set(GraphDatabaseInternalSettings.strict_config_validation_allow_duplicates, true);
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testStrictValidationForDuplicatesAllowDuplicates() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        GraphDatabaseSettings.initial_default_database.name() + "=foo",
                        GraphDatabaseSettings.initial_default_database.name() + "=bar"));
        Config.Builder builder = Config.newBuilder().fromFile(confFile);
        builder.set(GraphDatabaseSettings.strict_config_validation, true);
        builder.set(GraphDatabaseInternalSettings.strict_config_validation_allow_duplicates, true);
        MutableObject<Config> conf = new MutableObject<>();
        assertThatCode(() -> conf.setValue(builder.build())).doesNotThrowAnyException();

        var logProvider = new AssertableLogProvider();
        conf.getValue().setLogger(logProvider.getLog(Config.class));
        assertThat(logProvider).containsMessages("setting is overridden");
    }

    @Test
    void testIncorrectType() {
        Map<Setting<?>, Object> cfgMap = Map.of(TestSettings.intSetting, "not an int");
        Config.Builder builder =
                Config.newBuilder().addSettingsClass(TestSettings.class).set(cfgMap);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error evaluating value for setting 'db.test.setting.integer'."
                        + " Setting 'db.test.setting.integer' can not have value 'not an int'."
                        + " Should be of type 'Integer', but is 'String'");
    }

    @Test
    void testDoesNotLogChangedJvmArgs() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "server.jvm.additional=-XX:+UseG1GC",
                        "server.jvm.additional=-XX:+AlwaysPreTouch",
                        "server.jvm.additional=-XX:+UnlockExperimentalVMOptions",
                        "server.jvm.additional=-XX:+TrustFinalNonStaticFields"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(logProvider).doesNotHaveAnyLogs();
    }

    @Test
    void shouldCorrectlyValidateDependenciesInConstraints() {
        // Given
        Config.Builder builder = Config.emptyBuilder().addSettingsClass(ConstraintDependency.class);

        // Then
        assertThatCode(builder::build).doesNotThrowAnyException();

        builder.set(ConstraintDependency.setting1, 5);
        builder.set(ConstraintDependency.setting2, 3);
        assertThatCode(builder::build).doesNotThrowAnyException();

        builder.set(ConstraintDependency.setting2, 4);
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximum allowed value is 3");

        builder.set(ConstraintDependency.setting1, 2);
        assertThatCode(builder::build).doesNotThrowAnyException();
    }

    @Test
    void shouldCorrectlyValidateValueDependenciesInConstraints() {
        // Given
        Config.Builder builder = Config.emptyBuilder().addSettingsClass(ConstraintValueDependency.class);

        // Then
        assertThatCode(builder::build).doesNotThrowAnyException();

        // When
        builder.set(ConstraintValueDependency.setting1, Boolean.TRUE);

        // Then
        builder.set(ConstraintValueDependency.setting2, 1);
        assertThatCode(builder::build).doesNotThrowAnyException();

        builder.set(ConstraintValueDependency.setting2, 2);
        assertThatCode(builder::build).doesNotThrowAnyException();

        // When
        builder.set(ConstraintValueDependency.setting1, Boolean.FALSE);

        // Then
        builder.set(ConstraintValueDependency.setting2, 1);
        assertThatCode(builder::build).doesNotThrowAnyException();

        builder.set(ConstraintValueDependency.setting2, 2);
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2 is not allowed since 'dbms.test.setting.1' was false");
    }

    @Test
    void shouldFindCircularDependenciesInConstraints() {
        // Given
        Config.Builder builder = Config.emptyBuilder().addSettingsClass(CircularConstraints.class);

        // Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("circular dependency");
    }

    @Test
    void shouldNotAllowDependenciesOnDynamicSettings() {
        // Given
        Config.Builder builder = Config.emptyBuilder().addSettingsClass(DynamicConstraintDependency.class);

        // Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Can not depend on dynamic setting");
    }

    @Test
    void shouldNotEvaluateCommandsByDefault() {
        assumeUnixOrWindows();
        // Given
        Config.Builder builder = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .setRaw(Map.of(TestSettings.intSetting.name(), "$(foo bar)"));
        // Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is a command, but config is not explicitly told to expand it");
    }

    @Test
    void shouldReportCommandWithSyntaxError() {
        assumeUnixOrWindows();
        // Given
        Config.Builder builder = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .setRaw(Map.of(TestSettings.intSetting.name(), "$(foo bar"));
        // Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Error evaluating value for setting 'db.test.setting.integer'");
    }

    @Test
    void shouldReportUsefulErrorOnInvalidCommand() {
        assumeUnixOrWindows();
        // Given
        Config.Builder builder = Config.newBuilder()
                .allowCommandExpansion()
                .addSettingsClass(TestSettings.class)
                .setRaw(Map.of(TestSettings.intSetting.name(), "$(foo bar)"));
        // Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot run program \"foo\"");
    }

    @Test
    void shouldCorrectlyEvaluateCommandAndLogIt() {
        assumeUnixOrWindows();
        // Given
        var logProvider = new AssertableLogProvider();
        String command = IS_OS_WINDOWS ? "cmd.exe /c set /a" : "expr";
        Config config = Config.newBuilder()
                .allowCommandExpansion()
                .addSettingsClass(TestSettings.class)
                .setRaw(Map.of(TestSettings.intSetting.name(), format("$(%s 10 - 2)", command)))
                .build();
        config.setLogger(logProvider.getLog(Config.class));

        // Then
        assertThat(config.get(TestSettings.intSetting)).isEqualTo(8);
        assertThat(logProvider)
                .containsMessages(
                        "Command expansion is explicitly enabled for configuration",
                        "Executing external script to retrieve value of setting " + TestSettings.intSetting.name());
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void testThatFileAttributePermissionsDoNotWork() throws IOException {
        // Given
        Path confFile = testDirectory.file("test.conf");
        Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rw---x-w-");

        // When
        Files.createFile(confFile, PosixFilePermissions.asFileAttribute(permissions));

        // Then
        // we would expect that the created file has all the permissions that we asked for...
        assertThatThrownBy(() -> assertThat(Files.getPosixFilePermissions(confFile))
                        .containsExactlyInAnyOrderElementsOf(permissions))
                .isInstanceOf(AssertionError.class);
        // why would you do this to us java ?!
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void testThatFilesPosixFilePermissionsDoWork() throws IOException {
        // Given
        Path confFile = testDirectory.file("test.conf");
        Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rw---x-w-");

        // When
        Files.createFile(confFile);
        Files.setPosixFilePermissions(confFile, permissions);

        // Then
        assertThat(Files.getPosixFilePermissions(confFile)).containsExactlyInAnyOrderElementsOf(permissions);
    }

    @Test
    @DisabledOnOs(OS.WINDOWS) // For some reason it does not work on our test instances on TC
    void shouldCorrectlyEvaluateCommandFromFile() throws IOException {
        assumeUnixOrWindows();
        Path confFile = testDirectory.file("test.conf");
        Files.createFile(confFile);
        Files.write(confFile, List.of(format("%s=$(expr 3 + 3)", TestSettings.intSetting.name())));

        setPosixFilePermissions(confFile, permittedFilePermissionsForCommandExpansion);

        // Given
        Config config = Config.newBuilder()
                .allowCommandExpansion()
                .addSettingsClass(TestSettings.class)
                .fromFile(confFile)
                .build();

        // Then
        assertThat(config.get(TestSettings.intSetting)).isEqualTo(6);
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldHandleQuotesCorrectlyInCommandExpansion() throws IOException {
        Path confFile = testDirectory.file("test.conf");
        Files.createFile(confFile);
        Files.write(confFile, List.of(format("%s=$(bash -c \"echo '1'\")", TestSettings.stringSetting.name())));

        setPosixFilePermissions(confFile, permittedFilePermissionsForCommandExpansion);

        // Given
        Config config = Config.newBuilder()
                .allowCommandExpansion()
                .addSettingsClass(TestSettings.class)
                .fromFile(confFile)
                .build();

        // Then
        assertThat(config.get(TestSettings.stringSetting)).isEqualTo("1");
    }

    @Test
    void shouldNotEvaluateWithIncorrectFilePermission() throws IOException {
        assumeUnixOrWindows();
        Path confFile = testDirectory.file("test.conf");
        Files.createFile(confFile);
        Files.write(confFile, List.of(TestSettings.intSetting.name() + "=$(foo bar)"));

        if (IS_OS_WINDOWS) {
            AclFileAttributeView attrs = Files.getFileAttributeView(confFile, AclFileAttributeView.class);
            attrs.setAcl(List.of(AclEntry.newBuilder()
                    .setType(AclEntryType.ALLOW)
                    .setPrincipal(attrs.getOwner())
                    .setPermissions(
                            AclEntryPermission.READ_DATA,
                            AclEntryPermission.WRITE_DATA,
                            AclEntryPermission.READ_ATTRIBUTES,
                            AclEntryPermission.WRITE_ATTRIBUTES,
                            AclEntryPermission.READ_NAMED_ATTRS,
                            AclEntryPermission.WRITE_NAMED_ATTRS,
                            AclEntryPermission.APPEND_DATA,
                            AclEntryPermission.READ_ACL,
                            AclEntryPermission.SYNCHRONIZE,
                            AclEntryPermission.EXECUTE)
                    .build()));
        } else {
            setPosixFilePermissions(confFile, PosixFilePermissions.fromString("rw-----w-"));
        }

        // Given
        Config.Builder builder = Config.newBuilder()
                .allowCommandExpansion()
                .addSettingsClass(TestSettings.class)
                .fromFile(confFile);

        // Then
        String expectedErrorMessage = IS_OS_WINDOWS
                ? "does not have the correct ACL for owner"
                : "does not have the correct file permissions";
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expectedErrorMessage);
    }

    private static void setPosixFilePermissions(Path confFile, Set<PosixFilePermission> filePermissions)
            throws IOException {
        Files.setPosixFilePermissions(confFile, filePermissions);

        // It seems weird to assert here but when setting file permissions via FileAttributes the created files did not
        // have the permissions that we asked for.
        // So better to check explicitly here than to get really confused later.
        assertThat(Files.getPosixFilePermissions(confFile)).containsExactlyInAnyOrderElementsOf(filePermissions);
    }

    @Test
    @DisabledOnOs({OS.WINDOWS})
    void shouldNotEvaluateK8sConfDirWithIncorrectFilePermission() throws IOException {
        // Given
        Path confDir = createK8sStyleConfigDir(PosixFilePermissions.fromString("rw-----w-"));
        Config.Builder builder = Config.newBuilder()
                .allowCommandExpansion()
                .addSettingsClass(TestSettings.class)
                .fromFile(confDir);

        // Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not have the correct file permissions");
    }

    @Test
    @DisabledOnOs({OS.WINDOWS})
    void shouldEvaluateK8sConfDirWithCorrectFilePermission() throws IOException {
        var permittedPermissions = permittedFilePermissionsForCommandExpansion;

        // Given
        Path confDir = createK8sStyleConfigDir(permittedPermissions);

        var testSetting = Files.createFile(confDir.resolve(TestSettings.intSetting.name()));
        Files.write(testSetting, "$(expr 3 + 3)".getBytes());
        Files.setPosixFilePermissions(testSetting, permittedPermissions);

        Config.Builder configBuilder = Config.newBuilder()
                .allowCommandExpansion()
                .addSettingsClass(TestSettings.class)
                .fromFile(confDir);

        // Then
        Config config = buildWithoutErrorsOrWarnings(configBuilder::build);
        assertThat(config.get(TestSettings.intSetting)).isEqualTo(6);
    }

    @Test
    void shouldTimeoutOnSlowCommands() {
        assumeUnixOrWindows();
        String command = IS_OS_WINDOWS ? "ping -n 3 localhost" : "sleep 3";
        // This should be the only test modifying this value, so no issue of modifying feature flag
        FeatureToggles.set(Config.class, "CommandEvaluationTimeout", 1);
        // Given
        Config.Builder builder = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.config_command_evaluation_timeout, Duration.ofSeconds(1))
                .allowCommandExpansion()
                .addSettingsClass(TestSettings.class)
                .setRaw(Map.of(TestSettings.intSetting.name(), format("$(%s)", command)));
        // Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Timed out executing command");
    }

    @Test
    void shouldNotEvaluateCommandsOnDynamicChanges() {
        assumeUnixOrWindows();
        String command1 = format("$(%s 2 + 2)", IS_OS_WINDOWS ? "cmd.exe /c set /a" : "expr");
        String command2 = format("$(%s 10 - 3)", IS_OS_WINDOWS ? "cmd.exe /c set /a" : "expr");
        // Given
        Config config = Config.emptyBuilder()
                .allowCommandExpansion()
                .addSettingsClass(TestSettings.class)
                .setRaw(Map.of(TestSettings.dynamicStringSetting.name(), command1))
                .build();
        // Then
        assertThat(config.get(TestSettings.dynamicStringSetting)).isEqualTo("4");
        // When
        config.setDynamic(TestSettings.dynamicStringSetting, command2, "test");
        // Then
        assertThat(config.get(TestSettings.dynamicStringSetting)).isNotEqualTo("7"); // not evaluated
        assertThat(config.get(TestSettings.dynamicStringSetting)).isEqualTo(command2);
    }

    @Test
    void shouldHaveSettingSourceLocation() {
        // Given
        TestConnectionGroupSetting group = TestConnectionGroupSetting.group("default");
        Config config = Config.emptyBuilder()
                .addGroupSettingClass(TestConnectionGroupSetting.class)
                .addSettingsClass(TestSettings.class)
                .set(group.port, 7474)
                .build();

        // Then
        assertThat(((SettingImpl<?>) config.getSetting(TestSettings.intSetting.name())).sourceLocation())
                .isEqualTo("org.neo4j.configuration.ConfigTest.TestSettings.intSetting");
        assertThat(((SettingImpl<?>) config.getSetting(group.port.name())).sourceLocation())
                .isEqualTo("org.neo4j.configuration.ConfigTest.TestConnectionGroupSetting.port");
    }

    @Test
    void shouldConcatenateMultipleJvmAdditionals() {
        // Given
        Config config = Config.newBuilder()
                .setRaw(Map.of(BootloaderSettings.additional_jvm.name(), "-Dfoo"))
                .setRaw(Map.of(BootloaderSettings.additional_jvm.name(), "-Dbar"))
                .setRaw(Map.of(BootloaderSettings.additional_jvm.name(), "-Dbaz"))
                .build();

        // Then
        assertThat(config.get(BootloaderSettings.additional_jvm))
                .isEqualTo(String.format("%s%n%s%n%s", "-Dfoo", "-Dbar", "-Dbaz"));
    }

    /**
     * Ideally we'd generate all possible combinations of permissions but that requires some combinatorics library, which we don't have
     */
    private static Stream<Arguments> forbiddenFilePermissions() {
        return forbiddenFilePermissionsForCommandExpansion.stream().map(p -> Arguments.of(Set.of(p)));
    }

    /**
     * Check that the method we are using to generate test parameters does what we think it does.
     */
    @Test
    void testForbiddenFilePermissionsContainsAllNotPermittedPermissions() {
        Set<PosixFilePermission> invalidFilePermissions = forbiddenFilePermissions()
                .flatMap(a -> ((Set<PosixFilePermission>) a.get()[0]).stream())
                .collect(Collectors.toSet());

        // Any file permission that's not in the acceptable list is invalid - there's no middle ground. So all possible
        // permissions must be exist in either
        // the permitted list or the forbidden list.
        assertThat(Sets.union(invalidFilePermissions, permittedFilePermissionsForCommandExpansion))
                .containsExactlyInAnyOrderElementsOf(Arrays.asList(PosixFilePermission.values()));

        // This is just a sanity check
        assertThat(invalidFilePermissions).hasSize(6);

        // This is the most important one to check, this should never be valid
        assertThat(invalidFilePermissions).contains(OTHERS_WRITE);
    }

    @DisabledOnOs({OS.WINDOWS})
    @ParameterizedTest(name = "{0}")
    @MethodSource("forbiddenFilePermissions")
    void testForbiddenFilePermissionsShouldBeInvalidOnTheirOwn(Set<PosixFilePermission> forbidden) throws IOException {
        // Given
        Set<PosixFilePermission> readable =
                Set.of(OWNER_READ); // required otherwise the test will fail because we cannot read the file at all
        Path confFile = testDirectory.file("test.conf");
        Files.createFile(confFile);
        Files.write(confFile, List.of(format("%s=$(expr 3 + 3)", TestSettings.intSetting.name())));
        setPosixFilePermissions(confFile, Sets.union(readable, forbidden));
        Config.Builder builder = Config.newBuilder()
                .allowCommandExpansion()
                .addSettingsClass(TestSettings.class)
                .fromFile(confFile);

        // when/then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not have the correct file permissions to evaluate commands");
    }

    @DisabledOnOs({OS.WINDOWS})
    @ParameterizedTest(name = "{0}")
    @MethodSource("forbiddenFilePermissions")
    void testForbiddenFilePermissionsShouldBeInvalidWhenCombinedWithPermittedPermissions(
            Set<PosixFilePermission> forbidden) throws IOException {
        // Given
        Set<PosixFilePermission> permittedPermissions = permittedFilePermissionsForCommandExpansion;
        Path confFile = testDirectory.file("test.conf");
        Files.createFile(confFile);
        Files.write(confFile, List.of(format("%s=$(expr 3 + 3)", TestSettings.intSetting.name())));
        setPosixFilePermissions(confFile, Sets.union(permittedPermissions, forbidden));
        Config.Builder builder = Config.newBuilder()
                .allowCommandExpansion()
                .addSettingsClass(TestSettings.class)
                .fromFile(confFile);

        // when/then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not have the correct file permissions to evaluate commands");
    }

    @DisabledOnOs({OS.WINDOWS})
    @ParameterizedTest(name = "{0}")
    @MethodSource("forbiddenFilePermissions")
    void testForbiddenFilePermissionsShouldBeInvalidOnTheirOwnForK8sConfDir(Set<PosixFilePermission> forbidden)
            throws IOException {
        // Given
        Set<PosixFilePermission> readable =
                Set.of(OWNER_READ); // required otherwise the test will fail because we cannot read the file at all
        Path confDir = createK8sStyleConfigDir(Sets.union(readable, forbidden));
        Config.Builder builder = Config.newBuilder()
                .allowCommandExpansion()
                .addSettingsClass(TestSettings.class)
                .fromFile(confDir);

        // when/then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not have the correct file permissions to evaluate commands");
    }

    @DisabledOnOs({OS.WINDOWS})
    @ParameterizedTest(name = "{0}")
    @MethodSource("forbiddenFilePermissions")
    void testForbiddenFilePermissionsShouldBeInvalidWhenCombinedWithPermittedPermissionsForK8sConfDir(
            Set<PosixFilePermission> forbidden) throws IOException {
        // Given
        Set<PosixFilePermission> permittedPermissions = permittedFilePermissionsForCommandExpansion;
        Path confDir = createK8sStyleConfigDir(Sets.union(permittedPermissions, forbidden));
        Config.Builder builder = Config.newBuilder()
                .allowCommandExpansion()
                .addSettingsClass(TestSettings.class)
                .fromFile(confDir);

        // when/then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not have the correct file permissions to evaluate commands");
    }

    @Test
    void settingsLoadedInPredefinedOrder() {
        Config config = Config.newBuilder()
                .addSettingsClass(BSettings.class)
                .addSettingsClass(ASettings.class)
                .build();
        Object actual = config.settings.get("dbms.test.setting.marker");
        assertThat(actual.toString()).contains("bValue");
    }

    @Test
    void migratorsAppliedInPredefinedOrder() {
        Config config = Config.newBuilder()
                .addMigrator(new BMigrator())
                .addMigrator(new AMigrator())
                .build();

        assertThat(config.get(GraphDatabaseSettings.transaction_timeout)).isEqualTo(Duration.ofSeconds(777));
    }

    @Test
    void shouldKnowDefaultValue() {
        // Given
        Config config = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .set(TestSettings.intSetting, 77)
                .build();

        // Then
        assertThat(config.getDefault(TestSettings.intSetting)).isEqualTo(TestSettings.intSetting.defaultValue());

        // Given
        config = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .setDefault(TestSettings.intSetting, 50)
                .build();
        // Then
        assertThat(config.getDefault(TestSettings.intSetting)).isEqualTo(50);
    }

    @Test
    void shouldRememberStartupValue() {
        // Given
        Config config = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .set(TestSettings.intSetting, 77)
                .build();

        config.set(TestSettings.intSetting, 50);

        // Then
        assertThat(config.getStartupValue(TestSettings.intSetting)).isEqualTo(77);
        assertThat(config.getDefault(TestSettings.intSetting)).isEqualTo(1);
    }

    @Test
    void shouldKnowValueSource() {
        // Given
        Config config = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .set(TestSettings.boolSetting, false)
                .build();

        // When
        config.setDynamic(TestSettings.intSetting, 50, "Test");
        config.setDynamicByUser(TestSettings.dynamicStringSetting, "foo", "Test");

        // Then
        assertThat(config.getValueSource(TestSettings.stringSetting)).isEqualTo(ValueSource.DEFAULT);
        assertThat(config.getValueSource(TestSettings.boolSetting)).isEqualTo(ValueSource.INITIAL);
        assertThat(config.getValueSource(TestSettings.intSetting)).isEqualTo(ValueSource.SYSTEM);
        assertThat(config.getValueSource(TestSettings.dynamicStringSetting)).isEqualTo(ValueSource.USER);

        // Then
        // Scope remembered fromConfig
        config = Config.newBuilder().fromConfig(config).build();
        assertThat(config.getValueSource(TestSettings.stringSetting)).isEqualTo(ValueSource.DEFAULT);
        assertThat(config.getValueSource(TestSettings.boolSetting)).isEqualTo(ValueSource.INITIAL);
        assertThat(config.getValueSource(TestSettings.intSetting)).isEqualTo(ValueSource.SYSTEM);
        assertThat(config.getValueSource(TestSettings.dynamicStringSetting)).isEqualTo(ValueSource.USER);
    }

    private static final class AMigrator implements SettingMigrator {

        @Override
        public void migrate(Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            values.put(GraphDatabaseSettings.transaction_timeout.name(), "111s");
        }
    }

    private static final class BMigrator implements SettingMigrator {

        @Override
        public void migrate(Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            values.put(GraphDatabaseSettings.transaction_timeout.name(), "777s");
        }
    }

    private static final class ASettings implements SettingsDeclaration {
        static final Setting<String> stringSetting =
                newBuilder("dbms.test.setting.marker", STRING, "aValue").build();
    }

    private static final class BSettings implements SettingsDeclaration {
        static final Setting<String> stringSetting =
                newBuilder("dbms.test.setting.marker", STRING, "bValue").build();
    }

    private static final class TestSettings implements SettingsDeclaration {
        static final Setting<String> stringSetting =
                newBuilder("db.test.setting.string", STRING, "hello").build();
        static final Setting<String> dynamicStringSetting = newBuilder("db.test.setting.dynamicstring", STRING, "hello")
                .dynamic()
                .build();
        static final Setting<Integer> intSetting =
                newBuilder("db.test.setting.integer", INT, 1).dynamic().build();
        static final Setting<Integer> constrainedIntSetting = newBuilder("db.test.setting.constrained-integer", INT, 1)
                .addConstraint(max(3))
                .dynamic()
                .build();
        static final Setting<List<Integer>> intListSetting = newBuilder(
                        "db.test.setting.integerlist", listOf(INT), List.of(1))
                .build();
        static final Setting<Boolean> boolSetting =
                newBuilder("db.test.setting.bool", BOOL, null).immutable().build();
    }

    private static final class InternalWrongNamespaceSettings implements SettingsDeclaration {
        @Internal
        static final Setting<String> wrongInternalSetting = newBuilder(
                        "server.setting.not_really.internal", STRING, "hello")
                .build();
    }

    private static final class WrongNamespaceSettings implements SettingsDeclaration {
        static final Setting<Boolean> wrongSetting =
                newBuilder("planet.express.open", BOOL, false).build();
    }

    private static final class PublicWrongNamespaceSettings implements SettingsDeclaration {
        static final Setting<String> wrongPublicSetting =
                newBuilder("setting.not_really.internal", STRING, "hello").build();
    }

    private static final class LegacyUnsupportedNamespaceSettings implements SettingsDeclaration {
        static final Setting<String> wrongPublicSetting =
                newBuilder("setting.unsupported_or_not_really", STRING, "hello").build();
    }

    private static final class CircularConstraints implements SettingsDeclaration {
        private static final SettingConstraint<String> circular = new SettingConstraint<>() {
            @Override
            public void validate(String value, Configuration config) {
                config.get(CircularConstraints.setting2);
            }

            @Override
            public String getDescription() {
                return "circular test dependency";
            }
        };

        static final Setting<String> setting1 = newBuilder("db.test.setting.1", STRING, "aloha")
                .addConstraint(circular)
                .build();
        static final Setting<Integer> setting2 = newBuilder("db.test.setting.2", INT, 1)
                .addConstraint(dependency(max(3), max(5), setting1, is("aloha")))
                .build();
    }

    private static final class DynamicConstraintDependency implements SettingsDeclaration {
        static final Setting<Integer> setting1 =
                newBuilder("browser.test.setting.1", INT, 1).dynamic().build();
        static final Setting<Integer> setting2 = newBuilder("browser.test.setting.2", INT, 1)
                .addConstraint(dependency(max(3), unconstrained(), setting1, is(5)))
                .build();
    }

    private static final class ConstraintDependency implements SettingsDeclaration {
        static final Setting<Integer> setting1 =
                newBuilder("dbms.test.setting.1", INT, 1).build();
        static final Setting<Integer> setting2 = newBuilder("dbms.test.setting.2", INT, 1)
                .addConstraint(dependency(max(3), unconstrained(), setting1, is(5)))
                .build();
    }

    private static final class ConstraintValueDependency implements SettingsDeclaration {
        static final Setting<Boolean> setting1 =
                newBuilder("dbms.test.setting.1", BOOL, Boolean.TRUE).build();
        static final Setting<Integer> setting2 = newBuilder("dbms.test.setting.2", INT, 1)
                .addConstraint(valueDependency(List.of(2), setting1))
                .build();
    }

    public static class TestConnectionGroupSetting implements GroupSetting {
        private final String id;

        public static TestConnectionGroupSetting group(String name) {
            return new TestConnectionGroupSetting(name);
        }

        @Override
        public String name() {
            return id;
        }

        @Override
        public String getPrefix() {
            return "server.test.connection.http";
        }

        public final Setting<Integer> port;
        public final Setting<String> hostname;
        public final Setting<Boolean> secure;

        TestConnectionGroupSetting(String id) {
            this.id = id;
            port = getBuilder(getPrefix(), id, "port", INT, 1).build();
            hostname =
                    getBuilder(getPrefix(), id, "hostname", STRING, "0.0.0.0").build();
            secure = getBuilder(getPrefix(), id, "secure", BOOL, true).build();
        }
    }

    public static class TestDynamicGroupSetting implements GroupSetting {
        private final String id;

        public static TestDynamicGroupSetting group(String name) {
            return new TestDynamicGroupSetting(name);
        }

        @Override
        public String name() {
            return id;
        }

        @Override
        public String getPrefix() {
            return "dbms.test.dynamic";
        }

        public final Setting<String> value;

        public final Setting<String> constrainedValue;

        TestDynamicGroupSetting(String id) {
            this.id = id;
            value = getBuilder(getPrefix(), id, "value", STRING, "hello")
                    .dynamic()
                    .build();
            constrainedValue = getBuilder(getPrefix(), id, "constrainedValue", STRING, "aDefaultValue")
                    .addConstraint(SettingConstraints.matches("a.*"))
                    .dynamic()
                    .build();
        }
    }

    abstract static class ParentGroup implements GroupSetting {
        final Setting<String> parentSetting;
        private final String name;

        ParentGroup(String name) {
            this.name = name;
            parentSetting =
                    getBuilder(getPrefix(), name, "parent", STRING, "parent").build();
        }

        @Override
        public String name() {
            return name;
        }
    }

    static class ChildGroup extends ParentGroup {
        final Setting<String> childSetting;

        private ChildGroup(String name) {
            super(name);
            childSetting = getBuilder(getPrefix(), name, "child", STRING, null).build();
        }

        @Override
        public String getPrefix() {
            return "db.test.inheritance";
        }
    }

    abstract static class ParentDynamicGroup implements GroupSetting {
        final Setting<String> parentSetting;
        private final String name;

        ParentDynamicGroup(String name) {
            this.name = name;
            parentSetting = getBuilder(getPrefix(), name, "parent", STRING, "parent")
                    .dynamic()
                    .build();
        }

        @Override
        public String name() {
            return name;
        }
    }

    static class ChildDynamicGroup extends ParentDynamicGroup {
        final Setting<String> childSetting;

        private ChildDynamicGroup(String name) {
            super(name);
            childSetting = getBuilder(getPrefix(), name, "child", STRING, null)
                    .dynamic()
                    .build();
        }

        @Override
        public String getPrefix() {
            return "server.test.dynamic.inheritance";
        }
    }

    private static final class DependencySettings implements SettingsDeclaration {
        static final Setting<Path> basePath = newBuilder(
                        "db.test.base.path", PATH, Path.of("/base/").toAbsolutePath())
                .immutable()
                .build();
        static final Setting<Path> midPath = newBuilder("db.test.mid.path", PATH, Path.of("mid/"))
                .setDependency(basePath)
                .immutable()
                .build();
        static final Setting<Path> endPath = newBuilder("db.test.end.path", PATH, Path.of("end/file"))
                .setDependency(midPath)
                .build();
        static final Setting<Path> absolute = newBuilder(
                        "db.test.absolute.path",
                        PATH,
                        Path.of("/another/path/file").toAbsolutePath())
                .setDependency(midPath)
                .build();

        static final Setting<String> baseString = newBuilder("db.test.default.dependency.base", STRING, "base")
                .immutable()
                .build();

        static final Setting<String> dependingString = newBuilder("db.test.default.dependency.dep", STRING, null)
                .setDependency(baseString)
                .build();
    }

    private static void assumeUnixOrWindows() {
        assumeTrue(IS_OS_WINDOWS || SystemUtils.IS_OS_UNIX, "Require system to be either Unix or Windows based.");
    }
}
