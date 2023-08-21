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
import static org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor.ENGINE_ID;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedSuccessfully;
import static org.neo4j.internal.helpers.ProcessUtils.executeJava;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.time.Duration;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;

/**
 * Executes each test method in a forked JVM.
 */
public class ForkingTestExtension implements BeforeEachCallback, InvocationInterceptor {

    private static final int SUCCESS_CODE = 66;
    private static boolean inFork;
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        if (inFork) {
            return;
        }

        String testClass = extensionContext.getRequiredTestClass().getName();
        String methodName = extensionContext.getRequiredTestMethod().getName();
        int exitCode = executeJava(
                out, err, Duration.ofMinutes(20), ForkingTestExtension.class.getName(), testClass, methodName);

        assertThat(exitCode)
                .as("Out: %s %nErr: %s", out.toString(), err.toString())
                .isEqualTo(SUCCESS_CODE);
    }

    public static void main(String[] args) {
        inFork = true;
        String className = args[0];
        String methodName = args[1];
        // Use Junit to execute test, ensures all extensions and initialization is set up correctly
        Events testEvents = EngineTestKit.engine(ENGINE_ID)
                .selectors(selectMethod(className, methodName))
                .enableImplicitConfigurationParameters(true)
                .execute()
                .testEvents();

        testEvents.assertThatEvents().haveExactly(1, event(finishedSuccessfully()));
        System.exit(SUCCESS_CODE);
    }

    @Override
    public void interceptTestMethod(
            Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext,
            ExtensionContext extensionContext)
            throws Throwable {
        onlyRunInFork(invocation);
    }

    @Override
    public void interceptBeforeAllMethod(
            Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext,
            ExtensionContext extensionContext)
            throws Throwable {
        onlyRunInFork(invocation);
    }

    @Override
    public void interceptBeforeEachMethod(
            Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext,
            ExtensionContext extensionContext)
            throws Throwable {
        onlyRunInFork(invocation);
    }

    @Override
    public void interceptAfterEachMethod(
            Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext,
            ExtensionContext extensionContext)
            throws Throwable {
        onlyRunInFork(invocation);
    }

    @Override
    public void interceptAfterAllMethod(
            Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext,
            ExtensionContext extensionContext)
            throws Throwable {
        onlyRunInFork(invocation);
    }

    private static void onlyRunInFork(Invocation<Void> invocation) throws Throwable {
        if (inFork) {
            invocation.proceed();
        } else {
            invocation.skip();
        }
    }
}
