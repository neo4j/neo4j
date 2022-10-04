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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.setup.FactoryFunction;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.connection.transport.DefaultTransportSelector;
import org.neo4j.bolt.test.connection.transport.TransportSelector;
import org.neo4j.bolt.test.wire.initializer.BoltWireInitializer;
import org.neo4j.bolt.test.wire.selector.BoltWireSelector;
import org.neo4j.bolt.test.wire.selector.DefaultBoltWireSelector;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public final class BoltTestSupportExtension implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        var supportAnnotation = context.getTestMethod()
                .flatMap(method -> AnnotationSupport.findAnnotation(method, BoltTestExtension.class))
                .or(() -> context.getTestClass()
                        .flatMap(type -> AnnotationSupport.findAnnotation(type, BoltTestExtension.class)));

        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<? extends TestDatabaseManagementServiceBuilder> databaseFactoryType = supportAnnotation
                .map(BoltTestExtension::databaseManagementServiceBuilder)
                .filter(type -> BoltTestExtension.PlaceholderTestDatabaseManagementServiceBuilder.class != type)
                .orElseGet(() -> (Class) this.getDefaultDatabaseFactoryType());

        var factoryFunction = this.getFactoryFunction(context);
        var settingsFunction = this.getSettingsFunction(context);

        return this.getTransportTypes(context).flatMap(transportType -> this.getWires(context)
                .map(wire -> this.configure(
                        context, databaseFactoryType, factoryFunction, settingsFunction, transportType, wire)));
    }

    protected BoltTestConfig configure(
            ExtensionContext context,
            Class<? extends TestDatabaseManagementServiceBuilder> databaseFactoryType,
            Method factoryFunction,
            Method settingsFunction,
            TransportType transportType,
            BoltWire wire) {
        return new BoltTestConfig(databaseFactoryType, factoryFunction, settingsFunction, transportType, wire);
    }

    protected static Optional<Method> findMethod(Class<?> type, Class<? extends Annotation> annotationType) {
        var methods = AnnotationSupport.findAnnotatedMethods(type, annotationType, HierarchyTraversalMode.TOP_DOWN);
        if (methods.isEmpty()) {
            return Optional.empty();
        }

        if (methods.size() != 1) {
            Assertions.fail("Illegal test configuration: Only one method may be annotated with @"
                    + annotationType.getSimpleName() + ": " + methods.size() + " were found");
        }

        return Optional.of(methods.get(0));
    }

    protected Method getFactoryFunction(ExtensionContext context) {
        var method = context.getTestClass()
                .flatMap(type -> findMethod(type, FactoryFunction.class))
                .orElse(null);

        if (method == null) {
            return null;
        }

        Assertions.assertFalse(
                method.getParameterCount() > 1,
                "Method annotated with @" + FactoryFunction.class.getSimpleName()
                        + " is invalid: Must accept zero or one parameters");

        if (method.getParameterCount() == 1) {
            Assertions.assertTrue(
                    TestDatabaseManagementServiceBuilder.class.isAssignableFrom(method.getParameterTypes()[0]),
                    "Method annotated with @" + FactoryFunction.class.getSimpleName()
                            + " is invalid: Parameter must be "
                            + TestDatabaseManagementServiceBuilder.class.getSimpleName() + " or one of its children");
        } else {
            Assertions.assertTrue(
                    TestDatabaseManagementServiceBuilder.class.isAssignableFrom(method.getReturnType()),
                    "Method annotated with @" + FactoryFunction.class.getSimpleName()
                            + " is invalid: Return type must be "
                            + TestDatabaseManagementServiceBuilder.class.getSimpleName() + " or one of its children");
        }

        return method;
    }

    protected Method getSettingsFunction(ExtensionContext context) {
        var method = context.getTestClass()
                .flatMap(type -> findMethod(type, SettingsFunction.class))
                .orElse(null);

        if (method == null) {
            return null;
        }

        Assertions.assertFalse(
                method.getParameterCount() > 1,
                "Method annotated with @" + SettingsFunction.class.getSimpleName()
                        + " is invalid: Must accept one parameter");
        Assertions.assertEquals(
                Map.class,
                method.getParameterTypes()[0],
                "Method annotated with @" + SettingsFunction.class.getSimpleName()
                        + " is invalid: Parameter must be Map<" + Setting.class + "<?>, Object>");

        return method;
    }

    protected Class<? extends TestDatabaseManagementServiceBuilder> getDefaultDatabaseFactoryType() {
        return TestDatabaseManagementServiceBuilder.class;
    }

    protected Stream<BoltWire> getWires(ExtensionContext context) {
        var selector = BoltWireSelector.findSelector(context).orElseGet(DefaultBoltWireSelector::new);
        var initializers = BoltWireInitializer.findInitializer(context);

        return selector.select(context)
                .peek(wire -> initializers.forEach(initializer -> initializer.initialize(context, wire)));
    }

    protected Stream<TransportType> getTransportTypes(ExtensionContext context) {
        var selector = TransportSelector.findSelector(context).orElseGet(DefaultTransportSelector::new);

        return selector.select(context);
    }
}
