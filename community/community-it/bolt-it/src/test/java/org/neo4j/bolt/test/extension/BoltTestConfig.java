/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.test.extension;

import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.neo4j.bolt.test.extension.lifecycle.ServerInstanceManager;
import org.neo4j.bolt.test.extension.lifecycle.TransportConnectionManager;
import org.neo4j.bolt.test.extension.resolver.StaticParameterResolver;
import org.neo4j.bolt.test.extension.resolver.connection.ConnectionProviderParameterResolver;
import org.neo4j.bolt.test.extension.resolver.connection.HostnamePortParameterResolver;
import org.neo4j.bolt.test.extension.resolver.connection.TransportConnectionParameterResolver;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

/**
 * Encapsulates the configuration with which a given test method is to be invoked.
 *
 * @param databaseFactoryType database factory implementation class reference.
 * @param wire selected wire.
 * @param transport selected transport.
 */
record BoltTestConfig(
        Class<? extends TestDatabaseManagementServiceBuilder> databaseFactoryType,
        Method databaseFactoryFunction,
        Method databaseSettingsFunction,
        TransportType transport,
        BoltWire wire)
        implements TestTemplateInvocationContext {

    @Override
    public String getDisplayName(int invocationIndex) {
        return this.wire.getProtocolVersion() + " via " + this.transport.name();
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        var connectionManager = new TransportConnectionManager(this.transport);

        return List.of(
                new ServerInstanceManager(this::initializeServer),
                connectionManager,
                new StaticParameterResolver<>(BoltWire.class, this.wire),
                new StaticParameterResolver<>(TransportType.class, this.transport),
                new HostnamePortParameterResolver(),
                new ConnectionProviderParameterResolver(connectionManager, this.wire),
                new TransportConnectionParameterResolver(connectionManager, this.wire));
    }

    private void initializeServer(ExtensionContext context, Neo4jWithSocket server) {
        var factory = this.createDatabaseFactory(context);
        var settingsFunction = this.createSettingsFunction(context);

        server.setGraphDatabaseFactory(factory);
        server.setConfigure(settingsFunction);
    }

    /**
     * Retrieves a function capable of creating and customizing a database factory.
     *
     * @return a function.
     */
    private TestDatabaseManagementServiceBuilder createDatabaseFactory(ExtensionContext context) {
        var factoryType = this.databaseFactoryType;
        var function = this.databaseFactoryFunction;

        // if the given factory function does not accept parameters, it is expected to have a return type of
        // TestDatabaseManagementServiceBuilder or one of its children thus replacing the class reference within
        // the BoltTestSupport annotation
        if (function != null && function.getParameterCount() == 0) {
            var handle = unreflectDatabaseFactoryFunctionHandle(context, function);

            try {
                return (TestDatabaseManagementServiceBuilder) handle.invoke();
            } catch (Throwable ex) {
                Assertions.fail("Failed to invoke @FactoryFunction " + function.getName(), ex);
            }
        }

        // otherwise, we will have to construct the TestDatabaseManagementServiceBuilder based on the class
        // reference given to us
        Constructor<? extends TestDatabaseManagementServiceBuilder> factoryConstructor;
        try {
            factoryConstructor = factoryType.getDeclaredConstructor();
        } catch (NoSuchMethodException ex) {
            Assertions.fail(
                    "Illegal database factory type " + factoryType.getName()
                            + ": Missing default no-args constructor - Try creating a @FactoryFunction method instead",
                    ex);
            return null; // make the compiler happy
        }

        TestDatabaseManagementServiceBuilder factory;
        try {
            factory = factoryConstructor.newInstance();
        } catch (IllegalAccessException ex) {
            Assertions.fail(
                    "Illegal database factory type " + factoryType.getName()
                            + ": Inaccessible default no-args constructor - Try creationg a @FactoryFunction method instead",
                    ex);
            return null; // make the compiler happy
        } catch (InstantiationException | InvocationTargetException ex) {
            Assertions.fail("Failed to instantiate database factory type " + factoryType.getName(), ex);
            return null; // make the compiler happy
        }

        // if a factory function is specified, it is expected to accept exactly one parameter of type
        // TestDatabaseManagementServiceBuilder or one of its children in order to further customize its parameters
        if (function != null) {
            // we'll also ensure that the constructed instance is within expected bounds in order to prevent any
            // obscure method handle errors from failing the test
            var expectedFactoryType = function.getParameterTypes()[0];
            if (!expectedFactoryType.isInstance(factory)) {
                Assertions.fail("Failed to invoke @FactoryFunction " + function.getName()
                        + ": Expected factory of type " + expectedFactoryType.getName() + " but got "
                        + factory.getClass().getName());
            }

            var handle = unreflectDatabaseFactoryFunctionHandle(context, function);

            try {
                handle.invoke(factory);
            } catch (Throwable ex) {
                Assertions.fail("Failed to invoke @FactoryFunction " + function.getName(), ex);
            }
        }

        return factory;
    }

    private Consumer<Map<Setting<?>, Object>> createSettingsFunction(ExtensionContext context) {
        var baseFunction = this.createDefaultSettingsFunction(context);
        var function = this.databaseSettingsFunction;

        // when no settings function is provided by the test class, we'll return the base function as-is in order to
        // initialize the database with default settings for Bolt testing
        if (function == null) {
            return this.createDefaultSettingsFunction(context);
        }

        // otherwise, we'll allocate a method handle for the target function, bind it to the test instance (if
        // applicable) and append it to the base function to permit overriding of additional settings
        var handle = unreflectDatabaseFactoryFunctionHandle(context, function);

        return baseFunction.andThen(settings -> {
            try {
                handle.invoke(settings);
            } catch (Throwable ex) {
                Assertions.fail("Failed to invoke settings function", ex);
            }
        });
    }

    private Consumer<Map<Setting<?>, Object>> createDefaultSettingsFunction(ExtensionContext context) {
        return settings -> {
            settings.put(BoltConnector.enabled, true);
            settings.put(BoltConnector.encryption_level, OPTIONAL);
        };
    }

    private static MethodHandle unreflectDatabaseFactoryFunctionHandle(ExtensionContext context, Method function) {
        // mark the target function accessible in order to permit access to package-private functions within test
        // classes
        function.setAccessible(true);

        MethodHandle handle;
        try {
            handle = MethodHandles.lookup().unreflect(function);
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException("Cannot access @FactoryFunction " + function.getName(), ex);
        }

        if (Modifier.isStatic(function.getModifiers())) {
            return handle;
        }

        var instance = context.getRequiredTestInstance();
        return handle.bindTo(instance);
    }
}
