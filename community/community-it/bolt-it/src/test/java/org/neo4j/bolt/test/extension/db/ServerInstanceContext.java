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
package org.neo4j.bolt.test.extension.db;

import static org.assertj.core.api.Assertions.fail;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.AnnotationUtils;
import org.junit.platform.commons.util.ReflectionUtils.HierarchyTraversalMode;
import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.test.annotation.setup.CustomizeConfig;
import org.neo4j.bolt.test.annotation.setup.FactoryFunction;
import org.neo4j.bolt.test.connection.resolver.property.MutableTestPropertyContext;
import org.neo4j.bolt.test.connection.setup.SettingBuilder;
import org.neo4j.bolt.test.connection.setup.SettingCustomizer.Context;
import org.neo4j.bolt.testing.util.AnnotationUtil;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketSupportExtension;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings.ConfiguredProtocolVersion;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class ServerInstanceContext {
    private final MutableTestPropertyContext properties;
    private final Function<ExtensionContext, TestDatabaseManagementServiceBuilder> databaseFactory;
    private final List<BiConsumer<ExtensionContext, TestDatabaseManagementServiceBuilder>> factoryCustomizers;
    private final List<SettingConsumer> settingsCustomizers;

    public ServerInstanceContext(
            MutableTestPropertyContext properties,
            Function<ExtensionContext, TestDatabaseManagementServiceBuilder> databaseFactory,
            List<BiConsumer<ExtensionContext, TestDatabaseManagementServiceBuilder>> factoryCustomizers,
            List<SettingConsumer> settingsCustomizers) {
        this.properties = properties;
        this.databaseFactory = databaseFactory;
        this.factoryCustomizers = factoryCustomizers;
        this.settingsCustomizers = settingsCustomizers;
    }

    public static ServerInstanceContext forExtensionContext(
            ExtensionContext context,
            MutableTestPropertyContext properties,
            Class<? extends TestDatabaseManagementServiceBuilder> fallbackType,
            List<BiConsumer<ExtensionContext, TestDatabaseManagementServiceBuilder>> factoryCustomizers,
            List<SettingConsumer> settingsCustomizers) {
        var databaseFactory = findDatabaseFactory(context, fallbackType);
        var discoveredSettingsCustomizers = new ArrayList<>(settingsCustomizers);
        discoveredSettingsCustomizers.addAll(findSettingsFunctions(context));

        return new ServerInstanceContext(
                properties, databaseFactory, factoryCustomizers, discoveredSettingsCustomizers);
    }

    public static Function<ExtensionContext, TestDatabaseManagementServiceBuilder> findDatabaseFactory(
            ExtensionContext context,
            Function<ExtensionContext, TestDatabaseManagementServiceBuilder> fallbackSupplier) {
        var function = AnnotationUtils.findAnnotatedMethods(
                        context.getRequiredTestClass(), FactoryFunction.class, HierarchyTraversalMode.TOP_DOWN)
                .stream()
                .findFirst()
                .orElse(null);

        // if the given factory function does not accept parameters, it is expected to have a return type of
        // TestDatabaseManagementServiceBuilder or one of its children thus replacing the class reference within
        // the BoltTestSupport annotation
        if (function != null && function.getParameterCount() == 0) {
            return instanceContext -> {
                var handle = unreflectMethodHandle(function);

                try {
                    if (!Modifier.isStatic(function.getModifiers())) {
                        return (TestDatabaseManagementServiceBuilder)
                                handle.bindTo(instanceContext.getRequiredTestInstance())
                                        .invoke();
                    }

                    return (TestDatabaseManagementServiceBuilder) handle.invoke();
                } catch (Throwable ex) {
                    fail("Failed to invoke @FactoryFunction " + function.getName(), ex);
                    return null; // make the compiler happy
                }
            };
        }

        // if a factory function is specified, it is expected to accept exactly one parameter of
        // type TestDatabaseManagementServiceBuilder or one of its children in order to further
        // customize its parameters - in this case we will wrap the supplier function
        if (function != null) {
            var expectedFactoryType = function.getParameterTypes()[0];
            var handle = unreflectMethodHandle(function);

            return instanceContext -> {
                var factory = fallbackSupplier.apply(instanceContext);

                // we'll also ensure that the constructed instance is within expected bounds in
                // order to prevent any obscure method handle errors from failing the test
                if (!expectedFactoryType.isInstance(factory)) {
                    fail("Failed to invoke @FactoryFunction " + function.getName()
                            + ": Expected factory of type " + expectedFactoryType.getName() + " but got "
                            + factory.getClass().getName());
                }

                try {
                    if (!Modifier.isStatic(function.getModifiers())) {
                        handle.bindTo(instanceContext.getRequiredTestInstance()).invoke(factory);
                    } else {
                        handle.invoke(factory);
                    }
                } catch (Throwable ex) {
                    fail("Failed to invoke @FactoryFunction " + function.getName(), ex);
                }

                return factory;
            };
        }

        // if we could not locate a function to create or customize the factory, we'll return the
        // fallback supplier as-is
        return fallbackSupplier;
    }

    public static Function<ExtensionContext, TestDatabaseManagementServiceBuilder> findDatabaseFactory(
            ExtensionContext context, Class<? extends TestDatabaseManagementServiceBuilder> fallbackType) {
        return findDatabaseFactory(context, instanceContext -> {
            // when no factory function is defined within the test context we will have to construct
            // the TestDatabaseManagementServiceBuilder based on the class reference given to us
            Constructor<? extends TestDatabaseManagementServiceBuilder> factoryConstructor;
            try {
                factoryConstructor = fallbackType.getDeclaredConstructor();
            } catch (NoSuchMethodException ex) {
                fail(
                        "Illegal database factory type " + fallbackType.getName()
                                + ": Missing default no-args constructor - Try creating a @FactoryFunction method instead",
                        ex);
                return null; // make the compiler happy
            }

            try {
                return factoryConstructor.newInstance();
            } catch (IllegalAccessException ex) {
                fail(
                        "Illegal database factory type " + fallbackType.getName()
                                + ": Inaccessible default no-args constructor - Try creationg a @FactoryFunction method instead",
                        ex);
                return null; // make the compiler happy
            } catch (InstantiationException | InvocationTargetException ex) {
                fail("Failed to instantiate database factory type " + fallbackType.getName(), ex);
                return null; // make the compiler happy
            }
        });
    }

    private static MethodHandle unreflectMethodHandle(Method function) {
        // mark the target function accessible in order to permit access to package-private functions within test
        // classes
        function.setAccessible(true);

        MethodHandle handle;
        try {
            handle = MethodHandles.lookup().unreflect(function);
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException("Cannot access @FactoryFunction " + function.getName(), ex);
        }

        return handle;
    }

    public static List<SettingConsumer> findSettingsFunctions(ExtensionContext context) {
        return AnnotationUtil.findAllAnnotationsWithContext(context, CustomizeConfig.class).stream()
                .map(ctx -> {
                    var provider = AnnotationUtil.instantiateProvider(ctx.annotation(), CustomizeConfig::value);

                    return (SettingConsumer) (properties, settings) -> {
                        var c = new Context(
                                context,
                                properties,
                                context.getRequiredTestClass(),
                                ctx.targetMethod().orElseGet(context::getRequiredTestMethod));
                        provider.customize(c, settings);
                    };
                })
                .toList();
    }

    public Neo4jWithSocket configure(ExtensionContext context) {
        var neo4j = Neo4jWithSocketSupportExtension.getInstance(context);
        if (neo4j == null) {
            throw new IllegalStateException(
                    "Illegal test configuration: Expected @Neo4jWithSocket extension to be present");
        }

        neo4j.setGraphDatabaseFactory(this.databaseFactory.apply(context));
        neo4j.setConfigure(config -> {
            BoltProtocol.installed().stream()
                    .map(BoltProtocol::version)
                    .max(ProtocolVersion::compareTo)
                    .ifPresent(latest -> config.put(
                            BoltConnectorInternalSettings.max_protocol_version,
                            new ConfiguredProtocolVersion((int) latest.major(), (int) latest.minor())));

            var settings = SettingBuilder.wrap(config);
            this.settingsCustomizers.forEach(customizer -> customizer.accept(properties, settings));
        });

        return neo4j;
    }

    public void stop(ExtensionContext context) {
        var neo4j = Neo4jWithSocketSupportExtension.getInstance(context);
        if (neo4j != null) {
            neo4j.shutdownDatabase();
        }
    }

    @FunctionalInterface
    public interface SettingConsumer {

        void accept(MutableTestPropertyContext properties, SettingBuilder settings);
    }
}
