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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GroupSettingHelper.getBuilder;
import static org.neo4j.configuration.SettingConstraints.dependency;
import static org.neo4j.configuration.SettingConstraints.is;
import static org.neo4j.configuration.SettingConstraints.max;
import static org.neo4j.configuration.SettingConstraints.unconstrained;
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
        assertEquals("hello", config.get(TestSettings.stringSetting));
        assertEquals(1, config.get(TestSettings.intSetting));
        assertEquals(List.of(1), config.get(TestSettings.intListSetting));
        assertNull(config.get(TestSettings.boolSetting));
    }

    @Test
    void failToBuildConfigForSettingInWrongNamespace() {
        var e = assertThrows(IllegalArgumentException.class, () -> Config.newBuilder()
                .addSettingsClass(WrongNamespaceSettings.class)
                .build());
        assertThat(e)
                .hasMessageContaining(
                        "Setting: 'planet.express.open' name does not reside in any of the supported setting namespaces which are: dbms., db., browser., server., internal.");
    }

    @Test
    void buildConfigForSettingInWrongNamespaceWhenStrictDisabled() {
        assertDoesNotThrow(() -> Config.newBuilder()
                .addSettingsClass(WrongNamespaceSettings.class)
                .set(GraphDatabaseSettings.strict_config_validation, false)
                .build());
    }

    @Test
    void failToBuildConfigForInternalSettingInWrongNamespace() {
        var e = assertThrows(IllegalArgumentException.class, () -> Config.newBuilder()
                .addSettingsClass(InternalWrongNamespaceSettings.class)
                .build());
        assertThat(e)
                .hasMessageContaining(
                        "Setting: 'server.setting.not_really.internal' is internal but does not reside in the correct internal settings namespace.");
    }

    @Test
    void failToBuildConfigForPublicSettingInInternalNamespace() {
        var e = assertThrows(IllegalArgumentException.class, () -> {
            Config.newBuilder()
                    .addSettingsClass(PublicWrongNamespaceSettings.class)
                    .build();
        });
        assertThat(e)
                .hasMessageContaining(
                        "Setting: 'setting.not_really.internal' is not internal but using internal settings namespace.");
    }

    @Test
    void failToBuildConfigForPublicSettingInLegacyUnsupportedNamespace() {
        var e = assertThrows(IllegalArgumentException.class, () -> {
            Config.newBuilder()
                    .addSettingsClass(LegacyUnsupportedNamespaceSettings.class)
                    .build();
        });
        assertThat(e)
                .hasMessageContaining(
                        " Setting: 'setting.unsupported_or_not_really' is not internal but using internal settings namespace.");
    }

    @Test
    void testFetchAbsentSetting() {
        Config config = Config.newBuilder().addSettingsClass(TestSettings.class).build();
        Setting<Boolean> absentSetting =
                newBuilder("test.absent.bool", BOOL, null).build();
        assertThrows(IllegalArgumentException.class, () -> config.get(absentSetting));
    }

    @Test
    void testUpdateValue() {
        Config config = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .set(TestSettings.intSetting, 3)
                .build();
        assertEquals(3, config.get(TestSettings.intSetting));
        config.setDynamic(TestSettings.intSetting, 2, getClass().getSimpleName());
        assertEquals(2, config.get(TestSettings.intSetting));
        config.setDynamic(TestSettings.intSetting, null, getClass().getSimpleName());
        assertEquals(1, config.get(TestSettings.intSetting));
    }

    @Test
    void testSetConstrainedValue() {
        Config.Builder builder =
                Config.newBuilder().addSettingsClass(TestSettings.class).set(TestSettings.constrainedIntSetting, 4);
        assertThrows(IllegalArgumentException.class, builder::build);
        builder.set(TestSettings.constrainedIntSetting, 2);
        assertDoesNotThrow(builder::build);
    }

    @Test
    void testUpdateConstrainedValue() {
        Config config = Config.newBuilder().addSettingsClass(TestSettings.class).build();
        assertThrows(
                IllegalArgumentException.class,
                () -> config.setDynamic(
                        TestSettings.constrainedIntSetting, 4, getClass().getSimpleName()));
        assertEquals(1, config.get(TestSettings.constrainedIntSetting));
        assertDoesNotThrow(() -> config.setDynamic(
                TestSettings.constrainedIntSetting, 2, getClass().getSimpleName()));
    }

    @Test
    void testOverrideAbsentSetting() {
        Map<String, String> settings = Map.of("test.absent.bool", FALSE);
        Config.Builder builder = Config.newBuilder()
                .set(GraphDatabaseSettings.strict_config_validation, true)
                .addSettingsClass(TestSettings.class)
                .setRaw(settings);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void testOverrideDefault() {

        Map<Setting<?>, Object> overriddenDefaults =
                Map.of(TestSettings.stringSetting, "foo", TestSettings.intSetting, 11, TestSettings.boolSetting, true);

        Config config = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .setDefaults(overriddenDefaults)
                .build();

        assertEquals("foo", config.get(TestSettings.stringSetting));
        assertEquals(11, config.get(TestSettings.intSetting));
        assertEquals(true, config.get(TestSettings.boolSetting));
    }

    @Test
    void testUpdateStatic() {
        Config config = Config.newBuilder().addSettingsClass(TestSettings.class).build();
        assertThrows(
                IllegalArgumentException.class,
                () -> config.setDynamic(
                        TestSettings.stringSetting, "not allowed", getClass().getSimpleName()));
        assertEquals("hello", config.get(TestSettings.stringSetting));
        config.set(TestSettings.stringSetting, "allowed internally");
        assertEquals("allowed internally", config.get(TestSettings.stringSetting));
    }

    @Test
    void testUpdateImmutable() {
        Config config = Config.newBuilder().addSettingsClass(TestSettings.class).build();
        assertThrows(
                IllegalArgumentException.class,
                () -> config.setDynamic(
                        TestSettings.boolSetting, true, getClass().getSimpleName()));
        assertThrows(IllegalArgumentException.class, () -> config.set(TestSettings.boolSetting, true));
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

        assertEquals(0, observedOld.getValue());
        assertEquals(0, observedNew.getValue());

        config.setDynamic(TestSettings.intSetting, 2, getClass().getSimpleName());
        assertEquals(1, observedOld.getValue());
        assertEquals(2, observedNew.getValue());

        config.setDynamic(TestSettings.intSetting, 7, getClass().getSimpleName());
        assertEquals(2, observedOld.getValue());
        assertEquals(7, observedNew.getValue());

        config.removeListener(TestSettings.intSetting, listener);

        config.setDynamic(TestSettings.intSetting, 9, getClass().getSimpleName());
        assertEquals(2, observedOld.getValue());
        assertEquals(7, observedNew.getValue());

        assertThrows(
                IllegalArgumentException.class, () -> config.addListener(TestSettings.boolSetting, (oV, nV) -> {}));
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

        assertEquals(1111, config.get(g1.port));
        assertEquals(2222, config.get(g2.port));
        assertEquals(false, config.get(g1.secure));
        assertEquals(true, config.get(g2.secure));

        assertThrows(
                IllegalArgumentException.class,
                () -> config.get(TestConnectionGroupSetting.group("not_specified_id").port));
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

        assertEquals("value1", config.get(g1.value));
        assertEquals("value2", config.get(g2.value));

        config.setDynamic(g1.value, "new1", getClass().getSimpleName());
        assertEquals("new1", config.get(g1.value));
        assertEquals("value2", config.get(g2.value));

        config.setDynamic(g2.value, "new2", getClass().getSimpleName());
        assertEquals("new2", config.get(g2.value));

        var groups = config.getGroups(TestDynamicGroupSetting.class);
        assertEquals(2, groups.size());
        assertEquals("new1", config.get(groups.get("1").value));
        assertEquals("new2", config.get(groups.get("2").value));
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

        assertEquals("avalue1", config.get(g1.constrainedValue));
        assertEquals("aDefaultValue", config.get(g2.constrainedValue));

        config.setDynamic(g1.constrainedValue, "aNewValue", getClass().getSimpleName());
        assertEquals("aNewValue", config.get(g1.constrainedValue));
        assertEquals("aDefaultValue", config.get(g2.constrainedValue));

        assertThrows(
                IllegalArgumentException.class,
                () -> config.setDynamic(g2.constrainedValue, "new2", getClass().getSimpleName()));
        assertEquals("aDefaultValue", config.get(g2.constrainedValue));
        assertEquals("aNewValue", config.get(g1.constrainedValue));

        var groups = config.getGroups(TestDynamicGroupSetting.class);
        assertEquals(2, groups.size());
        assertEquals("aNewValue", config.get(groups.get("1").constrainedValue));
        assertEquals("aDefaultValue", config.get(groups.get("2").constrainedValue));
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

        assertEquals("value1", config1.get(g1.value));
        assertEquals("value2", config1.get(g2.value));

        Config config2 = Config.newBuilder()
                .addGroupSettingClass(TestDynamicGroupSetting.class)
                .set(TestDynamicGroupSetting.group("1").value, "value1")
                .set(TestDynamicGroupSetting.group("2").value, "value2")
                .build();

        assertEquals("value1", config2.get(g1.value));
        assertEquals("value2", config2.get(g2.value));

        config1.setDynamic(
                TestDynamicGroupSetting.group("1").value, "new1", getClass().getSimpleName());
        config1.setDynamic(
                TestDynamicGroupSetting.group("2").value, "new2", getClass().getSimpleName());

        var groups1 = config1.getGroups(TestDynamicGroupSetting.class);
        assertEquals(2, groups1.size());
        assertEquals("new1", config1.get(groups1.get("1").value));
        assertEquals("new2", config1.get(groups1.get("2").value));

        var groups2 = config2.getGroups(TestDynamicGroupSetting.class);
        assertEquals(2, groups2.size());
        assertEquals("value1", config2.get(groups2.get("1").value));
        assertEquals("value2", config2.get(groups2.get("2").value));
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
            assertEquals(oldValue, "value1");
            assertEquals(newValue, "new1");
        });
        config.addListener(g2.value, (oldValue, newValue) -> {
            assertEquals(oldValue, "value2");
            assertEquals(newValue, "new2");
        });

        config.setDynamic(g1.value, "new1", getClass().getSimpleName());
        assertEquals("new1", config.get(g1.value));
        assertEquals("value2", config.get(g2.value));

        config.setDynamic(g2.value, "new2", getClass().getSimpleName());
        assertEquals("new2", config.get(g2.value));

        var groups = config.getGroups(TestDynamicGroupSetting.class);
        assertEquals(2, groups.size());
        assertEquals("new1", config.get(groups.get("1").value));
        assertEquals("new2", config.get(groups.get("2").value));
    }

    @Test
    void testGroupInheritance() {
        ChildGroup group = new ChildGroup("1");
        Config config = Config.newBuilder()
                .addGroupSettingClass(ChildGroup.class)
                .set(group.childSetting, "child")
                .build();

        assertEquals("child", config.get(group.childSetting));
        assertEquals("parent", config.get(group.parentSetting));
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
        assertEquals("newParent", config.get(group1.parentSetting));
        assertEquals("parent", config.get(group2.parentSetting));

        config.setDynamic(group1.childSetting, "newChild", getClass().getSimpleName());
        assertEquals("newChild", config.get(group1.childSetting));
        assertEquals("child", config.get(group2.childSetting));

        assertEquals(
                "newChild", config.get(config.getGroups(ChildDynamicGroup.class).get("1").childSetting));
        assertEquals(
                "newParent",
                config.get(config.getGroups(ChildDynamicGroup.class).get("1").parentSetting));

        assertEquals(
                "child", config.get(config.getGroups(ChildDynamicGroup.class).get("2").childSetting));
        assertEquals(
                "parent", config.get(config.getGroups(ChildDynamicGroup.class).get("2").parentSetting));
    }

    @Test
    void testMalformedGroupSetting() {
        Map<String, String> settings = Map.of("dbms.test.connection.http..foo.bar", "1111");

        Config.Builder builder = Config.newBuilder()
                .set(GraphDatabaseSettings.strict_config_validation, true)
                .addGroupSettingClass(TestConnectionGroupSetting.class)
                .setRaw(settings);

        assertThrows(IllegalArgumentException.class, builder::build);
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
        assertEquals(Set.of("default", "1", "2"), groups.keySet());
        assertEquals(7474, config.get(groups.get("default").port));
        assertEquals(true, config.get(groups.get("2").secure));
    }

    @Test
    void testFromConfig() {
        Config fromConfig = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .setDefault(TestSettings.boolSetting, false)
                .set(TestSettings.intSetting, 3)
                .build();

        Config config1 = Config.newBuilder().fromConfig(fromConfig).build();
        assertEquals(3, config1.get(TestSettings.intSetting));
        assertEquals("hello", config1.get(TestSettings.stringSetting));

        Config config2 = Config.newBuilder()
                .fromConfig(fromConfig)
                .set(TestSettings.intSetting, 5)
                .build();

        assertEquals(5, config2.get(TestSettings.intSetting));

        Config config3 = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .fromConfig(fromConfig)
                .set(TestSettings.intSetting, 7)
                .build();

        assertEquals(7, config3.get(TestSettings.intSetting));
        assertEquals(false, config3.get(TestSettings.boolSetting));
    }

    @Test
    void shouldThrowIfMultipleFromConfig() {
        Config fromConfig = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .setDefault(TestSettings.boolSetting, false)
                .set(TestSettings.intSetting, 3)
                .build();

        assertThrows(IllegalArgumentException.class, () -> Config.newBuilder()
                .fromConfig(fromConfig)
                .fromConfig(fromConfig)
                .build());
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
        assertEquals(Set.of("default", "1"), groups1.keySet());
        assertEquals(7474, config1.get(groups1.get("default").port));

        Config config2 = Config.newBuilder()
                .fromConfig(fromConfig)
                .addGroupSettingClass(TestConnectionGroupSetting.class)
                .set(TestConnectionGroupSetting.group("1").port, 3333)
                .set(TestConnectionGroupSetting.group("2").port, 2222)
                .set(TestConnectionGroupSetting.group("2").hostname, "127.0.0.1")
                .build();

        var groups2 = config2.getGroups(TestConnectionGroupSetting.class);
        assertEquals(Set.of("default", "1", "2"), groups2.keySet());
        assertEquals(7474, config2.get(groups2.get("default").port));
        assertEquals(3333, config2.get(groups2.get("1").port));
        assertEquals(true, config2.get(groups2.get("default").secure));
        assertEquals(true, config2.get(groups2.get("2").secure));
    }

    @Test
    void testResolveDefaultSettingDependency() {
        Config.Builder builder = Config.newBuilder().addSettingsClass(DependencySettings.class);

        {
            Config config = builder.build();
            assertEquals(config.get(DependencySettings.baseString), config.get(DependencySettings.dependingString));
        }
        {
            String value = "default overrides dependency";
            builder.setDefault(DependencySettings.dependingString, value);
            Config config = builder.build();
            assertEquals(value, config.get(DependencySettings.dependingString));
        }

        {
            String value = "value overrides dependency";
            builder.set(DependencySettings.dependingString, value);
            Config config = builder.build();
            assertEquals(value, config.get(DependencySettings.dependingString));
        }
    }

    @Test
    void testResolvePathSettingDependency() {
        Config config =
                Config.newBuilder().addSettingsClass(DependencySettings.class).build();

        assertEquals(Path.of("/base/").toAbsolutePath(), config.get(DependencySettings.basePath));
        assertEquals(Path.of("/base/mid/").toAbsolutePath(), config.get(DependencySettings.midPath));
        assertEquals(Path.of("/base/mid/end/file").toAbsolutePath(), config.get(DependencySettings.endPath));
        assertEquals(Path.of("/another/path/file").toAbsolutePath(), config.get(DependencySettings.absolute));

        config.set(DependencySettings.endPath, Path.of("/path/another_file"));
        config.set(DependencySettings.absolute, Path.of("path/another_file"));
        assertEquals(Path.of("/path/another_file").toAbsolutePath(), config.get(DependencySettings.endPath));
        assertEquals(Path.of("/base/mid/path/another_file").toAbsolutePath(), config.get(DependencySettings.absolute));
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
        assertThrows(IllegalArgumentException.class, builder::build);
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

        assertEquals(3, config.getGroups(SingleSettingGroup.class).size());
        assertEquals("default value", config.get(SingleSettingGroup.group("default").singleSetting));
        assertEquals("foo", config.get(SingleSettingGroup.group("foo").singleSetting));
        assertEquals("bar", config.get(SingleSettingGroup.group("bar").singleSetting));
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
        assertTrue(confFile.toFile().createNewFile());
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
        Stream.of(config1, config2)
                .forEach(c -> assertEquals("foo", c.get(GraphDatabaseSettings.initial_default_database)));
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
        Config config1 = buildWithoutErrorsOrWarnings(Config.newBuilder().fromFile(confFile)::build);
        assertThat(config1.get(GraphDatabaseSettings.procedure_allowlist)).containsExactly(latin1String);

        // Try reading with UTF-8
        Config config2 = buildWithoutErrorsOrWarnings(
                Config.newBuilder().setFileCharset(StandardCharsets.UTF_8).fromFile(confFile)::build);
        assertThat(config2.get(GraphDatabaseSettings.procedure_allowlist)).containsExactly(unicodeString);
    }

    @Test
    void canReadEscapedCharsInPathsUnescapedFromFile() throws IOException {
        Path confFile = testDirectory.file("test.conf");
        Files.writeString(confFile, GraphDatabaseSettings.data_directory.name() + "=\\test\folder");

        Config conf = buildWithoutErrorsOrWarnings(Config.newBuilder().fromFile(confFile)::build);
        assertEquals(
                Path.of("/test/folder").toAbsolutePath(),
                conf.get(GraphDatabaseSettings.data_directory).toAbsolutePath());
    }

    @Test
    void canReadConfigDir() throws IOException {
        Path confDir = testDirectory.directory("test.conf");
        Path defaultDatabase = confDir.resolve(GraphDatabaseSettings.initial_default_database.name());
        Files.write(defaultDatabase, "foo".getBytes());

        Config config1 = buildWithoutErrorsOrWarnings(Config.newBuilder().fromFile(confDir)::build);
        Config config2 = buildWithoutErrorsOrWarnings(Config.newBuilder().fromFileNoThrow(confDir)::build);
        Stream.of(config1, config2)
                .forEach(c -> assertEquals("foo", c.get(GraphDatabaseSettings.initial_default_database)));
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

        Stream.of(config, config2).forEach(c -> {
            assertEquals("foo", c.get(GraphDatabaseSettings.initial_default_database));
            assertEquals(true, c.get(GraphDatabaseSettings.auth_enabled));
            assertEquals(4, c.get(GraphDatabaseSettings.auth_max_failed_attempts));
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
        assertThrows(IllegalArgumentException.class, () -> {
            Path confFile = testDirectory.file("test.conf");

            Config.emptyBuilder().fromFile(confFile).build();
        });
    }

    @Test
    @DisabledForRoot
    void mustThrowIfConfigFileCouldNotBeRead() throws IOException {
        Path confFile = testDirectory.file("test.conf");
        assertTrue(confFile.toFile().createNewFile());
        assumeTrue(confFile.toFile().setReadable(false));
        assertThrows(
                IllegalArgumentException.class,
                () -> Config.emptyBuilder().fromFile(confFile).build());
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

        assertFalse(config.get(BoltConnector.enabled));
        assertFalse(config.get(HttpConnector.enabled));
        assertFalse(config.get(HttpsConnector.enabled));
    }

    @Test
    void testAmendIfNotSet() {
        Config config = Config.newBuilder().addSettingsClass(TestSettings.class).build();
        config.setIfNotSet(TestSettings.intSetting, 77);
        assertEquals(77, config.get(TestSettings.intSetting));

        Config configWithSetting = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .set(TestSettings.intSetting, 66)
                .build();
        configWithSetting.setIfNotSet(TestSettings.intSetting, 77);
        assertEquals(66, configWithSetting.get(TestSettings.intSetting));
    }

    @Test
    void testIsExplicitlySet() {
        Config config =
                Config.emptyBuilder().addSettingsClass(TestSettings.class).build();
        assertFalse(config.isExplicitlySet(TestSettings.intSetting));
        config.set(TestSettings.intSetting, 77);
        assertTrue(config.isExplicitlySet(TestSettings.intSetting));

        Config configWithSetting = Config.emptyBuilder()
                .addSettingsClass(TestSettings.class)
                .set(TestSettings.intSetting, 66)
                .build();
        assertTrue(configWithSetting.isExplicitlySet(TestSettings.intSetting));
        configWithSetting.set(TestSettings.intSetting, null);
        assertFalse(configWithSetting.isExplicitlySet(TestSettings.intSetting));
    }

    @Test
    void testStrictValidationForGarbage() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, Collections.singletonList("some_unrecognized_garbage=true"));

        Config.Builder builder = Config.newBuilder().fromFile(confFile);
        builder.set(GraphDatabaseSettings.strict_config_validation, true);
        assertThrows(IllegalArgumentException.class, builder::build);

        builder.set(GraphDatabaseSettings.strict_config_validation, false);
        assertDoesNotThrow(builder::build);
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
        assertThrows(IllegalArgumentException.class, builder::build);

        builder.set(GraphDatabaseSettings.strict_config_validation, false);
        assertDoesNotThrow(builder::build);
    }

    @Test
    void testStrictValidationForGarbageAllowDuplicates() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, Collections.singletonList("some_unrecognized_garbage=true"));

        Config.Builder builder = Config.newBuilder().fromFile(confFile);
        builder.set(GraphDatabaseSettings.strict_config_validation, true);
        builder.set(GraphDatabaseInternalSettings.strict_config_validation_allow_duplicates, true);
        assertThrows(IllegalArgumentException.class, builder::build);
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
        assertDoesNotThrow(() -> conf.setValue(builder.build()));

        var logProvider = new AssertableLogProvider();
        conf.getValue().setLogger(logProvider.getLog(Config.class));
        assertThat(logProvider).containsMessages("setting is overridden");
    }

    @Test
    void testIncorrectType() {
        Map<Setting<?>, Object> cfgMap = Map.of(TestSettings.intSetting, "not an int");
        Config.Builder builder =
                Config.newBuilder().addSettingsClass(TestSettings.class).set(cfgMap);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);
        assertEquals(
                "Error evaluating value for setting 'db.test.setting.integer'."
                        + " Setting 'db.test.setting.integer' can not have value 'not an int'."
                        + " Should be of type 'Integer', but is 'String'",
                exception.getMessage());
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
        assertDoesNotThrow(builder::build);

        builder.set(ConstraintDependency.setting1, 5);
        builder.set(ConstraintDependency.setting2, 3);
        assertDoesNotThrow(builder::build);

        builder.set(ConstraintDependency.setting2, 4);
        String msg =
                assertThrows(IllegalArgumentException.class, builder::build).getMessage();
        assertThat(msg).contains("maximum allowed value is 3");

        builder.set(ConstraintDependency.setting1, 2);
        assertDoesNotThrow(builder::build);
    }

    @Test
    void shouldFindCircularDependenciesInConstraints() {
        // Given
        Config.Builder builder = Config.emptyBuilder().addSettingsClass(CircularConstraints.class);

        // Then
        String msg =
                assertThrows(IllegalArgumentException.class, builder::build).getMessage();
        assertThat(msg).contains("circular dependency");
    }

    @Test
    void shouldNotAllowDependenciesOnDynamicSettings() {
        // Given
        Config.Builder builder = Config.emptyBuilder().addSettingsClass(DynamicConstraintDependency.class);

        // Then
        String msg =
                assertThrows(IllegalArgumentException.class, builder::build).getMessage();
        assertThat(msg).contains("Can not depend on dynamic setting");
    }

    @Test
    void shouldNotEvaluateCommandsByDefault() {
        assumeUnixOrWindows();
        // Given
        Config.Builder builder = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .setRaw(Map.of(TestSettings.intSetting.name(), "$(foo bar)"));
        // Then
        String msg =
                assertThrows(IllegalArgumentException.class, builder::build).getMessage();
        assertThat(msg).contains("is a command, but config is not explicitly told to expand it");
    }

    @Test
    void shouldReportCommandWithSyntaxError() {
        assumeUnixOrWindows();
        // Given
        Config.Builder builder = Config.newBuilder()
                .addSettingsClass(TestSettings.class)
                .setRaw(Map.of(TestSettings.intSetting.name(), "$(foo bar"));
        // Then
        String msg =
                assertThrows(IllegalArgumentException.class, builder::build).getMessage();
        assertThat(msg).contains("Error evaluating value for setting 'db.test.setting.integer'");
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
        String msg =
                assertThrows(IllegalArgumentException.class, builder::build).getMessage();
        assertThat(msg).contains("Cannot run program \"foo\"");
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
        assertEquals(8, config.get(TestSettings.intSetting));
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
        assertThrows(AssertionError.class, () -> assertThat(Files.getPosixFilePermissions(confFile))
                .containsExactlyInAnyOrderElementsOf(permissions));
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
        assertEquals(6, config.get(TestSettings.intSetting));
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
        assertEquals("1", config.get(TestSettings.stringSetting));
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
        String msg =
                assertThrows(IllegalArgumentException.class, builder::build).getMessage();
        String expectedErrorMessage = IS_OS_WINDOWS
                ? "does not have the correct ACL for owner"
                : "does not have the correct file permissions";
        assertThat(msg).contains(expectedErrorMessage);
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
        String msg =
                assertThrows(IllegalArgumentException.class, builder::build).getMessage();
        String expectedErrorMessage = "does not have the correct file permissions";
        assertThat(msg).contains(expectedErrorMessage);
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
        assertEquals(6, config.get(TestSettings.intSetting));
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
        String msg =
                assertThrows(IllegalArgumentException.class, builder::build).getMessage();
        assertThat(msg).contains("Timed out executing command");
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

        // when
        String errorMessage =
                assertThrows(IllegalArgumentException.class, builder::build).getMessage();

        // then
        assertThat(errorMessage).contains("does not have the correct file permissions to evaluate commands");
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

        // when
        String errorMessage =
                assertThrows(IllegalArgumentException.class, builder::build).getMessage();

        // then
        assertThat(errorMessage).contains("does not have the correct file permissions to evaluate commands");
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

        // when
        String errorMessage =
                assertThrows(IllegalArgumentException.class, builder::build).getMessage();

        // then
        assertThat(errorMessage).contains("does not have the correct file permissions to evaluate commands");
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

        // when
        String errorMessage =
                assertThrows(IllegalArgumentException.class, builder::build).getMessage();

        // then
        assertThat(errorMessage).contains("does not have the correct file permissions to evaluate commands");
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

        assertEquals(Duration.ofSeconds(777), config.get(GraphDatabaseSettings.transaction_timeout));
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
