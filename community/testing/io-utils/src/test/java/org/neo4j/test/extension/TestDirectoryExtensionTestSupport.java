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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
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
        Class<? extends DirectoryExtensionLifecycleVerificationTest.AfterEachTestFail> getPerTestAfterEachClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithRealFs.PerClassAfterEachTest.class;
        }

        @Override
        Class<? extends DirectoryExtensionLifecycleVerificationTest.SecondTestFailTest> getPerMethodClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithRealFs.PerMethodTest.class;
        }

        @Override
        Class<? extends DirectoryExtensionLifecycleVerificationTest.AfterEachTestFail> getPerMethodAfterEachClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithRealFs.PerMethodAfterEachTest.class;
        }

        @Override
        Class<? extends DirectoryExtensionLifecycleVerificationTest.AllPassTest> getAllPassClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithRealFs.PerMethodAllPass.class;
        }

        @Test
        @EnabledOnOs(OS.LINUX)
        @DisabledForRoot
        void exceptionOnDirectoryDeletionIncludeTestDisplayName() throws IOException {
            ExecutionSharedContext.clear();
            FailedTestExecutionListener failedTestListener = new FailedTestExecutionListener();
            execute("lockFileAndFailToDeleteDirectory", failedTestListener);
            Path lockedFile = ExecutionSharedContext.getValue(LOCKED_TEST_FILE_KEY);

            assertThat(lockedFile).isNotNull();
            assertThat(lockedFile.toFile().setReadable(true, true)).isTrue();
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
        Class<? extends DirectoryExtensionLifecycleVerificationTest.AfterEachTestFail> getPerTestAfterEachClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithEphemeralFs.PerClassAfterEachTest.class;
        }

        @Override
        Class<? extends DirectoryExtensionLifecycleVerificationTest.SecondTestFailTest> getPerMethodClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithEphemeralFs.PerMethodTest.class;
        }

        @Override
        Class<? extends DirectoryExtensionLifecycleVerificationTest.AfterEachTestFail> getPerMethodAfterEachClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithEphemeralFs.PerMethodAfterEachTest.class;
        }

        @Override
        Class<? extends DirectoryExtensionLifecycleVerificationTest.AllPassTest> getAllPassClass() {
            return DirectoryExtensionLifecycleVerificationTest.WithEphemeralFs.PerMethodAllPass.class;
        }

        // EphemeralFs + PER_METHOD: anyFailure is sticky on the shared TestDirectory, so
        // tests that pass AFTER a failure also have files copied to real FS.
        // Only check that failed tests' files are preserved (passing tests' file existence
        // depends on execution order relative to the first failure — pre-existing flakiness).
        @Override
        @Test
        void perMethodNestedShouldKeepAllFilesWhenAnyTestFails() {
            List<Pair<Path, Boolean>> pairs = executeAndReturnCreatedFiles(getPerMethodClass(), 6);
            for (var pair : pairs) {
                if (pair.other()) {
                    assertThat(pair.first()).exists();
                }
            }
        }

        // WithEphemeralFs is PER_METHOD (not PER_CLASS like WithRealFs) because
        // FileSystemExtension.afterAll() closes the EphemeralFS when fired for nested class
        // contexts, which breaks the deferred cleanup that PER_CLASS relies on.
        // The validation correctly rejects the PER_METHOD+PER_CLASS pattern,
        // so these tests verify the rejection instead of the lifecycle behavior.
        @Override
        @Test
        void failedTestShouldKeepDirectoryInPerClassLifecycle() {
            FailureCaptureListener listener = new FailureCaptureListener();
            LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                    .selectors(selectClass(getPerTestClass()))
                    .configurationParameter(TEST_TOGGLE, "true")
                    .build();
            Launcher launcher = LauncherFactory.create();
            launcher.execute(request, listener);
            assertThat(listener.getFailure())
                    .isInstanceOf(ExtensionConfigurationException.class)
                    .hasMessageContaining("PER_CLASS lifecycle")
                    .hasMessageContaining("PER_METHOD lifecycle");
        }

        @Override
        @Test
        void failedTestAfterEachShouldKeepDirectoryInPerClassLifecycle() {
            FailureCaptureListener listener = new FailureCaptureListener();
            LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                    .selectors(selectClass(getPerTestAfterEachClass()))
                    .configurationParameter(TEST_TOGGLE, "true")
                    .build();
            Launcher launcher = LauncherFactory.create();
            launcher.execute(request, listener);
            assertThat(listener.getFailure())
                    .isInstanceOf(ExtensionConfigurationException.class)
                    .hasMessageContaining("PER_CLASS lifecycle")
                    .hasMessageContaining("PER_METHOD lifecycle");
        }
    }

    @Test
    void testDirectoryInjectionWorks() {
        assertThat(testDirectory).isNotNull();
    }

    @Test
    void testDirectoryInitialisedForUsage() {
        Path directory = testDirectory.homePath();
        assertThat(directory).isNotNull();
        assertThat(fileSystem.fileExists(directory)).isTrue();
        Path targetTestData = Paths.get("target", "test data");
        assertThat(directory.toAbsolutePath().toString()).contains(targetTestData.toString());
    }

    @Test
    void testDirectoryUsesFileSystemFromExtension() {
        assertThat(testDirectory.getFileSystem()).isSameAs(fileSystem);
    }

    @Test
    void createTestFile() {
        Path file = testDirectory.createFile("a");
        assertThat(file.getFileName()).hasToString("a");
        assertThat(fileSystem.fileExists(file)).isTrue();
    }

    @Test
    void failedTestShouldKeepDirectory() {
        ExecutionSharedContext.clear();
        execute("failAndKeepDirectory");
        Path failedFile = ExecutionSharedContext.getValue(CREATED_TEST_FILE_PAIRS_KEY);
        assertThat(failedFile).isNotNull();
        assertThat(Files.exists(failedFile)).isTrue();
    }

    @Test
    void successfulTestShouldCleanupDirectory() {
        ExecutionSharedContext.clear();
        execute("executeAndCleanupDirectory");
        Path greenTestFail = ExecutionSharedContext.getValue(SUCCESSFUL_TEST_FILE_KEY);
        assertThat(greenTestFail).isNotNull();
        assertThat(Files.exists(greenTestFail)).isFalse();
    }

    @Test
    void failedTestShouldKeepDirectoryInPerClassLifecycle() {
        List<Pair<Path, Boolean>> pairs = executeAndReturnCreatedFiles(getPerTestClass(), 6);
        for (var pair : pairs) {
            assertThat(pair.first()).exists();
        }
    }

    @Test
    void failedTestAfterEachShouldKeepDirectoryInPerClassLifecycle() {
        List<Pair<Path, Boolean>> pairs = executeAndReturnCreatedFiles(getPerTestAfterEachClass(), 1);
        for (var pair : pairs) {
            assertThat(pair.first()).exists();
        }
    }

    @Test
    void failedTestAfterEachShouldKeepDirectoryInPerMethodLifecycle() {
        List<Pair<Path, Boolean>> pairs = executeAndReturnCreatedFiles(getPerMethodAfterEachClass(), 1);
        for (var pair : pairs) {
            assertThat(pair.first()).exists();
        }
    }

    @Test
    void perMethodNestedShouldKeepAllFilesWhenAnyTestFails() {
        List<Pair<Path, Boolean>> pairs = executeAndReturnCreatedFiles(getPerMethodClass(), 6);
        for (var pair : pairs) {
            assertThat(pair.first()).exists();
        }
    }

    /**
     * All-pass run: TestDirectory cleanup fires uniformly in both RealFs and EphemeralFs
     * because the sticky anyFailure flag stays false, so no ephemeral override is needed
     * unlike the mixed pass/fail test. If cleanup regresses, the ephemeral fallback that
     * copies files to real FS before deleting the ephemeral directory would fire instead,
     * so doesNotExist() against the returned path catches the regression in both variants.
     */
    @Test
    void successfulTestsShouldCleanupFilesInPerMethodLifecycle() {
        List<Pair<Path, Boolean>> pairs = executeAndReturnCreatedFiles(getAllPassClass(), 3);
        for (var pair : pairs) {
            assertThat(pair.first()).doesNotExist();
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

    @Test
    void frameworkRejectsPerMethodOuterWithPerClassNestedTestDirectory() {
        FailureCaptureListener listener = new FailureCaptureListener();
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass(PerMethodOuterWithPerClassNested.class))
                .configurationParameter(TEST_TOGGLE, "true")
                .build();
        Launcher launcher = LauncherFactory.create();
        launcher.execute(request, listener);
        assertThat(listener.getFailure())
                .isInstanceOf(ExtensionConfigurationException.class)
                .hasMessageContaining("PER_CLASS lifecycle")
                .hasMessageContaining("PER_METHOD lifecycle");
    }

    private static List<Pair<Path, Boolean>> executeAndReturnCreatedFiles(Class<?> testClass, int count) {
        ExecutionSharedContext.clear();
        executeClass(testClass);
        List<Pair<Path, Boolean>> pairs = ExecutionSharedContext.getValue(CREATED_TEST_FILE_PAIRS_KEY);
        assertThat(pairs).isNotNull().hasSize(count);
        return pairs;
    }

    abstract Class<? extends DirectoryExtensionLifecycleVerificationTest> getTestClass();

    abstract Class<? extends DirectoryExtensionLifecycleVerificationTest.SecondTestFailTest> getPerTestClass();

    abstract Class<? extends DirectoryExtensionLifecycleVerificationTest.AfterEachTestFail> getPerTestAfterEachClass();

    abstract Class<? extends DirectoryExtensionLifecycleVerificationTest.SecondTestFailTest> getPerMethodClass();

    abstract Class<? extends DirectoryExtensionLifecycleVerificationTest.AfterEachTestFail>
            getPerMethodAfterEachClass();

    abstract Class<? extends DirectoryExtensionLifecycleVerificationTest.AllPassTest> getAllPassClass();

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

    @ExtendWith(DirectoryExtensionLifecycleVerificationTest.ConfigurationParameterCondition.class)
    @ResourceLock(ExecutionSharedContext.SHARED_RESOURCE)
    @TestDirectoryExtension
    static class PerMethodOuterWithPerClassNested {
        @Inject
        TestDirectory directory;

        @Nested
        @TestInstance(TestInstance.Lifecycle.PER_CLASS)
        class PerClassInner {
            @Test
            void shouldNeverRun() {
                throw new AssertionError("Expected ExtensionConfigurationException before reaching this point");
            }
        }
    }

    private static class FailureCaptureListener implements TestExecutionListener {
        private Throwable failure;

        @Override
        public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
            if (testExecutionResult.getStatus() == FAILED) {
                testExecutionResult.getThrowable().ifPresent(t -> failure = t);
            }
        }

        Throwable getFailure() {
            return failure;
        }
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
                assertThat(exceptionMessage).contains("Fail to cleanup test directory for WithRealFs");
            }
        }

        void assertTestObserver() {
            assertThat(resultsObserved).isEqualTo(1);
        }
    }
}
