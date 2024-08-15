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
package org.neo4j.test.extension.testdirectory;

import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.test.utils.TestDirectory.testDirectory;

import java.io.IOException;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.DynamicTestInvocationContext;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.junit.platform.commons.JUnitException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.FileSystemExtension;
import org.neo4j.test.extension.StatefulFieldExtension;
import org.neo4j.test.utils.TestDirectory;
import org.opentest4j.TestAbortedException;

public class TestDirectorySupportExtension extends StatefulFieldExtension<TestDirectory>
        implements BeforeEachCallback,
                BeforeAllCallback,
                AfterEachCallback,
                AfterAllCallback,
                TestExecutionExceptionHandler,
                InvocationInterceptor {
    public static final String TEST_DIRECTORY = "testDirectory";
    public static final String FAILURE_MARKER = "failureMarker";
    public static final String DYNAMIC_TEST_FAILURE_MARKER = "dynamicTestFailureMarker";
    public static final Namespace TEST_DIRECTORY_NAMESPACE = Namespace.create(TEST_DIRECTORY);
    private static final String JUNIT4_ASSUMPTION_EXCEPTION = "org.junit.AssumptionViolatedException";

    @Override
    public void beforeAll(ExtensionContext context) throws IOException {
        if (getLifecycle(context) == PER_CLASS) {
            prepare(context);
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws IOException {
        if (getLifecycle(context) == PER_METHOD) {
            prepare(context);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (getLifecycle(context) == PER_METHOD) {
            cleanUp(context);
        }
    }

    @Override
    public void interceptDynamicTest(
            Invocation<Void> inv, DynamicTestInvocationContext invContext, ExtensionContext context) throws Throwable {
        // All dynamic tests in one @TestFactory share the same lifecycle calls and test instance. E.g
        // beforeEach <-- creates the instance
        //  DynamicTest1 executes
        //  DynamicTest2 executes
        //  DynamicTestN executes
        // afterEach <-- cleans up the instance
        // The dynamic tests have individual contexts, with the same parent context (the one called before/afterEach)
        // If one fails (throws) the parent context is still successful, so we need to mark it as failed to keep the
        // files
        try {
            // Note that this is callback intercepts the test and behaves differently from the other callbacks.
            // It is very important that the original exception on failure is the one rethrown
            InvocationInterceptor.super.interceptDynamicTest(inv, invContext, context);
        } catch (Throwable throwable) {
            try {
                if (!isTestAssumptionCheckFailure(throwable)) {
                    getTestDirectoryStore(context.getParent().orElseThrow()).put(DYNAMIC_TEST_FAILURE_MARKER, TRUE);
                }
            } catch (RuntimeException e) {
                throwable.addSuppressed(e);
            }
            throw throwable;
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws IOException {
        if (getLifecycle(context) == PER_CLASS) {
            cleanUp(context);
        }

        var directory = getStoredValue(context);
        if (directory != null) {
            directory.close();
        }
    }

    private static TestInstance.Lifecycle getLifecycle(ExtensionContext context) {
        return context.getTestInstanceLifecycle().orElse(PER_METHOD);
    }

    public void prepare(ExtensionContext context) throws IOException {
        String name = context.getTestMethod()
                .map(method -> method.getName().concat(context.getDisplayName()))
                .orElseGet(() -> context.getRequiredTestClass().getSimpleName());
        TestDirectory testDirectory = getStoredValue(context);
        testDirectory.prepareDirectory(context.getRequiredTestClass(), name);
    }

    private void cleanUp(ExtensionContext context) {
        TestDirectory testDirectory = getStoredValue(context);
        try {
            testDirectory.complete((context.getExecutionException().isEmpty()
                            || isTestAssumptionCheckFailure(
                                    context.getExecutionException().get()))
                    && !hasFailureMarker(context));
        } catch (Exception e) {
            throw new JUnitException(
                    format("Fail to cleanup test directory for %s test.", context.getDisplayName()), e);
        }
    }

    @Override
    protected String getFieldKey() {
        return TEST_DIRECTORY;
    }

    @Override
    protected Class<TestDirectory> getFieldType() {
        return TestDirectory.class;
    }

    @Override
    protected TestDirectory createField(ExtensionContext extensionContext) {
        ExtensionContext.Store fileSystemStore = getStore(extensionContext, FileSystemExtension.FILE_SYSTEM_NAMESPACE);
        FileSystemAbstraction fileSystemAbstraction =
                fileSystemStore.get(FileSystemExtension.FILE_SYSTEM, FileSystemAbstraction.class);
        return fileSystemAbstraction != null ? testDirectory(fileSystemAbstraction) : testDirectory();
    }

    @Override
    protected Namespace getNameSpace() {
        return TEST_DIRECTORY_NAMESPACE;
    }

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        if (!isTestAssumptionCheckFailure(throwable)) {
            if (getLifecycle(context) == PER_CLASS) {
                var store = getTestDirectoryStore(context);
                store.put(FAILURE_MARKER, TRUE);
            }
        }
        throw throwable;
    }

    private static boolean isTestAssumptionCheckFailure(Throwable throwable) {
        // Junit5
        if (throwable instanceof TestAbortedException) {
            return true;
        }
        // Hacky Junit4 compat
        return throwable.getClass().getCanonicalName().equals(JUNIT4_ASSUMPTION_EXCEPTION);
    }

    private boolean hasFailureMarker(ExtensionContext context) {
        ExtensionContext.Store store = getLocalStore(context);
        return getLifecycle(context) == PER_CLASS && store.get(FAILURE_MARKER) != null
                || store.get(DYNAMIC_TEST_FAILURE_MARKER) != null;
    }

    private ExtensionContext.Store getTestDirectoryStore(ExtensionContext context) {
        ExtensionContext.Store store = null;
        while (context != null) {
            var localStore = context.getStore(getNameSpace());
            if (localStore.get(getFieldKey()) == null) {
                return store;
            } else {
                store = localStore;
            }
            context = context.getParent().orElse(null);
        }
        throw new IllegalStateException("Test directory store not found");
    }
}
