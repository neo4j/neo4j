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
package org.neo4j.bolt.testing.extension;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTest;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTestExtension;
import org.neo4j.bolt.testing.extension.dependency.MockStateMachineDependencyProvider;
import org.neo4j.bolt.testing.extension.dependency.StateMachineDependencyProvider;
import org.neo4j.bolt.testing.fsm.StateMachineProvider;
import org.neo4j.bolt.testing.util.AnnotationUtil;

public class StateMachineTestSupportExtension implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(ExtensionContext extensionContext) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
            ExtensionContext extensionContext) {
        var annotation = AnnotationUtil.findAnnotation(extensionContext, StateMachineTestExtension.class)
                .orElse(null);

        Class<? extends StateMachineDependencyProvider> dependencyProviderClass;
        if (annotation != null) {
            dependencyProviderClass = annotation.dependencyProvider();
        } else {
            dependencyProviderClass = MockStateMachineDependencyProvider.class;
        }

        return AnnotationUtil.findAnnotation(extensionContext, StateMachineTest.class)
                .map(this::matchingProviders)
                .orElseGet(StateMachineProvider::versions)
                .map(fsmProvider -> {
                    StateMachineDependencyProvider dependencyProvider;
                    try {
                        try {
                            dependencyProvider = dependencyProviderClass
                                    .getDeclaredConstructor(ExtensionContext.class)
                                    .newInstance(extensionContext);
                        } catch (NoSuchMethodException ignore) {
                            dependencyProvider = dependencyProviderClass
                                    .getDeclaredConstructor()
                                    .newInstance();
                        }
                    } catch (NoSuchMethodException
                            | InstantiationException
                            | IllegalAccessException
                            | InvocationTargetException ex) {
                        throw new IllegalStateException(
                                "Illegal test configuration: Cannot construct dependency provider", ex);
                    }

                    return new StateMachineInvocationContext(dependencyProvider, fsmProvider);
                });
    }

    private Stream<StateMachineProvider> matchingProviders(StateMachineTest annotation) {
        var includedVersions = StateMachineProvider.versions();

        var since = convertVersion(annotation.since());
        var until = convertVersion(annotation.until());

        if (since.major() != 0) {
            return includedVersions.filter(version -> version.version().compareTo(since) >= 0);
        }
        if (until.major() != ProtocolVersion.MAX_MINOR_BIT) {
            return includedVersions.filter(version -> version.version().compareTo(until) < 0);
        }

        if (annotation.include().length != 0) {
            var inclusions = Arrays.stream(annotation.include())
                    .map(this::convertVersion)
                    .toList();

            includedVersions = includedVersions.filter(
                    provider -> inclusions.stream().anyMatch(included -> included.matches(provider.version())));
        }

        if (annotation.exclude().length != 0) {
            var exclusions = Arrays.stream(annotation.exclude())
                    .map(this::convertVersion)
                    .toList();

            includedVersions = includedVersions.filter(
                    provider -> exclusions.stream().noneMatch(excluded -> excluded.matches(provider.version())));
        }

        return includedVersions;
    }

    private ProtocolVersion convertVersion(Version annotation) {
        if (annotation.minor() == -1) {
            return new ProtocolVersion(
                    annotation.major(), ProtocolVersion.MAX_MINOR_BIT, ProtocolVersion.MAX_MINOR_BIT);
        }

        return new ProtocolVersion(annotation.major(), annotation.minor(), annotation.range());
    }
}
