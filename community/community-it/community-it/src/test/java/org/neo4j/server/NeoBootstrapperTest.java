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
package org.neo4j.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllLines;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.management.MemoryUsage;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.memory.MachineMemory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutput;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(SuppressOutputExtension.class)
@ResourceLock(Resources.SYSTEM_OUT)
class NeoBootstrapperTest {
    @Inject
    private TestDirectory homeDir;

    @Inject
    private SuppressOutput suppress;

    private NeoBootstrapper neoBootstrapper;
    private Path dir;
    private Path userLog;

    @AfterEach
    void tearDown() throws IOException {
        if (neoBootstrapper != null) {
            neoBootstrapper.stop();
            // Even if we didn't start a database management system because of a configuration error we should log a
            // stopped message to allow users to
            // distinguish between a neo4j process that completed shutdown and one that was terminated without
            // performing shutdown
            assertThat(getUserLogFiles()).last().asString().endsWith("Stopped.");
        }
    }

    @BeforeEach
    void setUp() {
        dir = homeDir.directory("test-server-bootstrapper");
        userLog = dir.resolve("logs").resolve("neo4j.log");
    }

    @Test
    void shouldNotThrowNullPointerExceptionIfConfigurationValidationFails() {
        // given
        neoBootstrapper = new CommunityBootstrapperWithoutStartingServer();

        // when
        assertThatThrownBy(() -> neoBootstrapper.start(dir, Map.of("initial.dbms.default_database", "$%^&*#)@!")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasNoSuppressedExceptions()
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class);

        // then no exceptions are thrown on stop and logs are written
        neoBootstrapper.stop();
        assertThat(suppress.getOutputVoice().lines()).last().asString().endsWith("Stopped.");
        neoBootstrapper = null;
    }

    @Test
    void shouldFailToStartIfRequestedPageCacheMemoryExceedsTotal() throws IOException {
        // given
        neoBootstrapper = new CommunityBootstrapperWithoutStartingServer();
        Map<String, String> config = MapUtil.stringMap();
        config.put(GraphDatabaseSettings.pagecache_memory.name(), "10B");

        // Mock heap usage and free memory.
        MachineMemory mockedMemory = mock(MachineMemory.class);
        MemoryUsage heapMemory = new MemoryUsage(0, 1, 1, 1);
        when(mockedMemory.getTotalPhysicalMemory()).thenReturn(8L);
        when(mockedMemory.getHeapMemoryUsage()).thenReturn(heapMemory);
        neoBootstrapper.setMachineMemory(mockedMemory);

        assertThat(neoBootstrapper.start(dir, config)).isNotEqualTo(NeoBootstrapper.OK);

        assertThat(getUserLogFiles())
                .anyMatch(line -> line.contains("Invalid memory configuration - exceeds physical memory."));
    }

    @Test
    void shouldFailToStartIfRequestedHeapMemoryExceedsTotal() throws IOException {
        // given
        neoBootstrapper = new CommunityBootstrapperWithoutStartingServer();
        Map<String, String> config = MapUtil.stringMap();
        config.put(BootloaderSettings.max_heap_size.name(), "10B");
        config.put(GraphDatabaseSettings.pagecache_memory.name(), "1B");

        // Mock heap usage and free memory.
        MachineMemory mockedMemory = mock(MachineMemory.class);
        MemoryUsage heapMemory = new MemoryUsage(0, 1, 1, 10);
        when(mockedMemory.getTotalPhysicalMemory()).thenReturn(8L);
        when(mockedMemory.getHeapMemoryUsage()).thenReturn(heapMemory);
        neoBootstrapper.setMachineMemory(mockedMemory);

        assertThat(neoBootstrapper.start(dir, config)).isNotEqualTo(NeoBootstrapper.OK);
        assertThat(getUserLogFiles())
                .anyMatch(line -> line.contains("Invalid memory configuration - exceeds physical memory."));
    }

    @Test
    void shouldFailToStartIfRequestedHeapAndPageCacheMemoryExceedsTotal() throws IOException {
        // given
        neoBootstrapper = new CommunityBootstrapperWithoutStartingServer();
        Map<String, String> config = MapUtil.stringMap();
        config.put(BootloaderSettings.max_heap_size.name(), "10B");
        config.put(GraphDatabaseSettings.pagecache_memory.name(), "10B");

        // Mock heap usage and free memory.
        MachineMemory mockedMemory = mock(MachineMemory.class);
        MemoryUsage heapMemory = new MemoryUsage(0, 1, 1, 10);
        when(mockedMemory.getTotalPhysicalMemory()).thenReturn(18L);
        when(mockedMemory.getHeapMemoryUsage()).thenReturn(heapMemory);
        neoBootstrapper.setMachineMemory(mockedMemory);

        assertThat(neoBootstrapper.start(dir, config)).isNotEqualTo(NeoBootstrapper.OK);
        assertThat(getUserLogFiles())
                .anyMatch(line -> line.contains("Invalid memory configuration - exceeds physical memory."));
    }

    @Test
    void shouldFailToStartIfCalculatedPageCacheSizeExceedsTotalMemory() throws IOException {
        // given
        neoBootstrapper = new CommunityBootstrapperWithoutStartingServer();
        Map<String, String> config = MapUtil.stringMap();
        config.put(BootloaderSettings.max_heap_size.name(), "10B");

        // Mock heap usage and free memory.
        MachineMemory mockedMemory = mock(MachineMemory.class);
        MemoryUsage heapMemory = new MemoryUsage(0, 1, 1, 10);
        when(mockedMemory.getTotalPhysicalMemory()).thenReturn(1000L);
        when(mockedMemory.getHeapMemoryUsage()).thenReturn(heapMemory);
        neoBootstrapper.setMachineMemory(mockedMemory);

        assertThat(neoBootstrapper.start(dir, config)).isNotEqualTo(NeoBootstrapper.OK);
        assertThat(getUserLogFiles())
                .anyMatch(line -> line.contains("Invalid memory configuration - exceeds physical memory."));
    }

    @Test
    void ignoreMemoryChecksIfTotalMemoryIsNotAvailable() throws IOException {
        // given
        neoBootstrapper = new CommunityBootstrapperWithoutStartingServer();
        Map<String, String> config = MapUtil.stringMap();

        // Mock heap usage and free memory.
        MachineMemory mockedMemory = mock(MachineMemory.class);
        MemoryUsage heapMemory = new MemoryUsage(0, 1, 1, 10);
        when(mockedMemory.getTotalPhysicalMemory()).thenReturn(0L);
        when(mockedMemory.getHeapMemoryUsage()).thenReturn(heapMemory);
        neoBootstrapper.setMachineMemory(mockedMemory);

        assertThat(neoBootstrapper.start(dir, config)).isEqualTo(NeoBootstrapper.OK);
        assertThat(getUserLogFiles())
                .anyMatch(line -> line.contains("Unable to determine total physical memory of machine."));
    }

    private static Stream<Arguments> memoryConfigs() {
        return Stream.of(
                Arguments.of(35, false, true), // Heap in bad range
                Arguments.of(16, true, false), // Normal heap
                Arguments.of(64, false, false), // Huge heap
                Arguments.of(
                        35, true, false) // Heap in bad range but has COOPS, likely using -XX:ObjectAlignmentInBytes
                );
    }

    @ParameterizedTest
    @MethodSource("memoryConfigs")
    void shouldWarnAboutCompressedOOPSWhenDisabledWithHeapInBadRange(
            long heapInGB, boolean compressedOOPS, boolean expectWarning) throws IOException {
        neoBootstrapper = new CommunityBootstrapperWithoutStartingServer();

        MachineMemory mockedMemory = mock(MachineMemory.class);
        when(mockedMemory.getTotalPhysicalMemory()).thenReturn(ByteUnit.gibiBytes(128));
        when(mockedMemory.getHeapMemoryUsage()).thenReturn(new MemoryUsage(0, 1, 1, ByteUnit.gibiBytes(heapInGB)));
        when(mockedMemory.hasCompressedOOPS()).thenReturn(compressedOOPS);
        neoBootstrapper.setMachineMemory(mockedMemory);

        assertThat(neoBootstrapper.start(dir, Map.of())).isEqualTo(NeoBootstrapper.OK);
        Predicate<String> hasWarning = line -> line.contains("disabling of compressed ordinary object pointers");
        if (expectWarning) {
            assertThat(getUserLogFiles()).anyMatch(hasWarning);
        } else {
            assertThat(getUserLogFiles()).noneMatch(hasWarning);
        }
    }

    @Test
    void printLoggingConfig() throws IOException {
        neoBootstrapper = new CommunityBootstrapperWithoutStartingServer();
        assertThat(neoBootstrapper.start(dir, Map.of())).isEqualTo(NeoBootstrapper.OK);
        neoBootstrapper.stop();
        assertThat(getUserLogFiles()).anyMatch(line -> line.contains("Logging config in use: "));
    }

    private List<String> getUserLogFiles() throws IOException {
        return readAllLines(userLog, UTF_8);
    }

    private static class CommunityBootstrapperWithoutStartingServer extends CommunityBootstrapper {
        @Override
        protected DatabaseManagementService createNeo(
                Config config, boolean daemonMode, GraphDatabaseDependencies dependencies) {
            return mock(DatabaseManagementService.class);
        }
    }
}
