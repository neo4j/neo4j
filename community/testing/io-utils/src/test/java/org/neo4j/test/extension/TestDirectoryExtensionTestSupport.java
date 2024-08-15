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
package org.neo4j.test.extension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.platform.engine.TestExecutionResult.Status.FAILED;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;
import static org.neo4j.test.extension.DirectoryExtensionLifecycleVerificationTest.ConfigurationParameterCondition.TEST_TOGGLE;
import static org.neo4j.test.extension.ExecutionSharedContext.CREATED_TEST_FILE_PAIRS_KEY;
import static org.neo4j.test.extension.ExecutionSharedContext.LOCKED_TEST_FILE_KEY;
import static org.neo4j.test.extension.ExecutionSharedContext.SUCCESSFUL_TEST_FILE_KEY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@ResourceLock(ExecutionSharedContext.SHARED_RESOURCE)
abstract class TestDirectoryExtensionTestSupport {
    @Inject
    TestDirectory testDirectory;

    @Inject
    FileSystemAbstraction fileSystem;

    @TestDirectoryExtension
    static class WithRealFs extends TestDirectoryExtensionTestSupport {
        @Override
        Class<? extends DirectoryExtensionLifecycleVerificationTest> getTestClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithRealFs.class;
        }

        @Override
        Class<? extends DirectoryExtensionLifecycleVerificationTest.SecondTestFailTest> getPerTestClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithRealFs.PerClassTest.class;
        }

        @Override
        Class<? extends DirectoryExtensionLifecycleVerificationTest.SecondTestFailTest> getPerMethodClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithRealFs.PerMethodTest.class;
        }

        @Test
        @EnabledOnOs(OS.LINUX)
        @DisabledForRoot
        void exceptionOnDirectoryDeletionIncludeTestDisplayName() throws IOException {
            ExecutionSharedContext.clear();
            FailedTestExecutionListener failedTestListener = new FailedTestExecutionListener();
            execute("lockFileAndFailToDeleteDirectory", failedTestListener);
            Path lockedFile = ExecutionSharedContext.getValue(LOCKED_TEST_FILE_KEY);

            assertNotNull(lockedFile);
            assertTrue(lockedFile.toFile().setReadable(true, true));
            FileUtils.deleteDirectory(lockedFile);
            failedTestListener.assertTestObserver();
        }
    }

    @EphemeralTestDirectoryExtension
    static class WithEphemeralFs extends TestDirectoryExtensionTestSupport {
        @Override
        Class<? extends DirectoryExtensionLifecycleVerificationTest> getTestClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithEphemeralFs.class;
        }

        @Override
        Class<? extends DirectoryExtensionLifecycleVerificationTest.SecondTestFailTest> getPerTestClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithEphemeralFs.PerClassTest.class;
        }

        @Override
        Class<? extends DirectoryExtensionLifecycleVerificationTest.SecondTestFailTest> getPerMethodClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithEphemeralFs.PerMethodTest.class;
        }
    }

    @Test
    void testDirectoryInjectionWorks() {
        assertNotNull(testDirectory);
    }

    @Test
    void testDirectoryInitialisedForUsage() {
        Path directory = testDirectory.homePath();
        assertNotNull(directory);
        assertTrue(fileSystem.fileExists(directory));
        Path targetTestData = Paths.get("target", "test data");
        assertTrue(directory.toAbsolutePath().toString().contains(targetTestData.toString()));
    }

    @Test
    void testDirectoryUsesFileSystemFromExtension() {
        assertSame(fileSystem, testDirectory.getFileSystem());
    }

    @Test
    void createTestFile() {
        Path file = testDirectory.createFile("a");
        assertEquals("a", file.getFileName().toString());
        assertTrue(fileSystem.fileExists(file));
    }

    @Test
    void failedTestShouldKeepDirectory() {
        ExecutionSharedContext.clear();
        execute("failAndKeepDirectory");
        Path failedFile = ExecutionSharedContext.getValue(CREATED_TEST_FILE_PAIRS_KEY);
        assertNotNull(failedFile);
        assertTrue(Files.exists(failedFile));
    }

    @Test
    void successfulTestShouldCleanupDirectory() {
        ExecutionSharedContext.clear();
        execute("executeAndCleanupDirectory");
        Path greenTestFail = ExecutionSharedContext.getValue(SUCCESSFUL_TEST_FILE_KEY);
        assertNotNull(greenTestFail);
        assertFalse(Files.exists(greenTestFail));
    }

    @Test
    void failedTestShouldKeepDirectoryInPerClassLifecycle() {
        List<Pair<Path, Boolean>> pairs = executeAndReturnCreatedFiles(getPerTestClass(), 6);
        for (var pair : pairs) {
            assertThat(pair.first()).exists();
        }
    }

    @Test
    void failedTestShouldNotKeepDirectoryInPerMethodLifecycle() {
        List<Pair<Path, Boolean>> pairs = executeAndReturnCreatedFiles(getPerMethodClass(), 6);
        for (var pair : pairs) {
            if (pair.other()) {
                assertThat(pair.first()).exists();
            } else {
                assertThat(pair.first()).doesNotExist();
            }
        }
    }

    @Test
    void successfulDynamicTestsShouldCleanupDirectory() {
        ExecutionSharedContext.clear();
        execute("dynamicTests");
        assertThat(Files.exists(ExecutionSharedContext.getValue(CREATED_TEST_FILE_PAIRS_KEY)))
                .isFalse();
    }

    @Test
    void failedDynamicTestsShouldKeepDirectory() {
        ExecutionSharedContext.clear();
        execute("dynamicTestsWithFailure");
        Path path = ExecutionSharedContext.getValue(CREATED_TEST_FILE_PAIRS_KEY);
        assertThat(Files.isDirectory(path)).isTrue();
        assertThat(FileUtils.listPaths(path)).hasSize(3);
    }

    private static List<Pair<Path, Boolean>> executeAndReturnCreatedFiles(Class<?> testClass, int count) {
        ExecutionSharedContext.clear();
        executeClass(testClass);
        List<Pair<Path, Boolean>> pairs = ExecutionSharedContext.getValue(CREATED_TEST_FILE_PAIRS_KEY);
        assertNotNull(pairs);
        assertThat(pairs.size()).isEqualTo(count);
        return pairs;
    }

    abstract Class<? extends DirectoryExtensionLifecycleVerificationTest> getTestClass();

    abstract Class<? extends DirectoryExtensionLifecycleVerificationTest.SecondTestFailTest> getPerTestClass();

    abstract Class<? extends DirectoryExtensionLifecycleVerificationTest.SecondTestFailTest> getPerMethodClass();

    protected void execute(String testName, TestExecutionListener... testExecutionListeners) {
        LauncherDiscoveryRequest discoveryRequest = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectMethod(getTestClass(), testName))
                .configurationParameter(TEST_TOGGLE, "true")
                .build();
        execute(discoveryRequest, testExecutionListeners);
    }

    private static void executeClass(Class testClass, TestExecutionListener... testExecutionListeners) {
        LauncherDiscoveryRequest discoveryRequest = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass(testClass))
                .configurationParameter(TEST_TOGGLE, "true")
                .build();
        execute(discoveryRequest, testExecutionListeners);
    }

    private static void execute(
            LauncherDiscoveryRequest discoveryRequest, TestExecutionListener... testExecutionListeners) {
        Launcher launcher = LauncherFactory.create();
        launcher.execute(discoveryRequest, testExecutionListeners);
    }

    private static class FailedTestExecutionListener implements TestExecutionListener {
        private int resultsObserved;

        @Override
        public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
            if (testExecutionResult.getStatus() == FAILED) {
                resultsObserved++;
                String exceptionMessage = testExecutionResult
                        .getThrowable()
                        .map(Throwable::getMessage)
                        .orElse("");
                assertThat(exceptionMessage)
                        .contains("Fail to cleanup test directory for lockFileAndFailToDeleteDirectory");
            }
        }

        void assertTestObserver() {
            assertEquals(1, resultsObserved);
        }
    }
}
