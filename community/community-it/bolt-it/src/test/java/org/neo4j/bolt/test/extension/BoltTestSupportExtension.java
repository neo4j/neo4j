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
package org.neo4j.bolt.test.extension;

import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.api.extension.TestWatcher;
import org.junit.platform.commons.support.AnnotationSupport;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.connection.transport.DefaultTransportSelector;
import org.neo4j.bolt.test.connection.transport.TransportSelector;
import org.neo4j.bolt.test.extension.db.ServerInstanceContext;
import org.neo4j.bolt.test.extension.error.TestRetryException;
import org.neo4j.bolt.test.extension.store.RetryInfo;
import org.neo4j.bolt.test.wire.initializer.BoltWireInitializer;
import org.neo4j.bolt.test.wire.selector.BoltWireSelector;
import org.neo4j.bolt.test.wire.selector.DefaultBoltWireSelector;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public final class BoltTestSupportExtension implements TestTemplateInvocationContextProvider, TestWatcher {

    private static final Namespace NAMESPACE = Namespace.create(BoltTestSupportExtension.class);
    private static final String ITERATOR_KEY = "test_iterator";

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

        var instanceContext = ServerInstanceContext.forExtensionContext(
                context, databaseFactoryType, Collections.emptyList(), List.of((ctx, settings) -> {
                    settings.put(BoltConnector.enabled, true);
                    settings.put(BoltConnector.encryption_level, OPTIONAL);
                }));

        var templates = this.getTransportTypes(context)
                .flatMap(transportType -> this.getWires(context)
                        .map(wire -> this.configure(databaseFactoryType, instanceContext, transportType, wire)))
                .iterator();

        var it = new TestTemplateIterator(templates);

        var store = context.getStore(NAMESPACE);
        store.put(ITERATOR_KEY, it);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.NONNULL), false);
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        if (cause instanceof TestRetryException ex) {

            var store = context.getStore(NAMESPACE);
            var it = store.get(ITERATOR_KEY, TestTemplateIterator.class);
            if (it == null) {
                return;
            }

            var info = ex.getRetryInfo();
            it.markForRetry(info);
        }
    }

    protected BoltTestConfig configure(
            Class<? extends TestDatabaseManagementServiceBuilder> databaseFactoryType,
            ServerInstanceContext instanceContext,
            TransportType transportType,
            BoltWire wire) {
        return new BoltTestConfig(databaseFactoryType, instanceContext, transportType, wire);
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

    private static final class TestTemplateIterator implements Iterator<TestTemplateInvocationContext> {

        private final Iterator<BoltTestConfig> delegate;
        private BoltTestConfig previous;
        private RetryInfo retry;

        public TestTemplateIterator(Iterator<BoltTestConfig> delegate) {
            this.delegate = delegate;
        }

        public void markForRetry(RetryInfo info) {
            this.retry = info;
        }

        @Override
        public boolean hasNext() {
            if (this.retry != null && this.previous != null) {
                return true;
            }

            return this.delegate.hasNext();
        }

        @Override
        public TestTemplateInvocationContext next() {
            var retry = this.retry;
            this.retry = null;

            if (retry != null && this.previous != null) {
                return new RenamingTestTemplateInvocationContext(this.previous, retry);
            }

            var next = this.delegate.next();
            this.previous = next;
            return next;
        }
    }

    private static final class RenamingTestTemplateInvocationContext implements TestTemplateInvocationContext {

        private final TestTemplateInvocationContext delegate;
        private final RetryInfo retry;

        public RenamingTestTemplateInvocationContext(TestTemplateInvocationContext delegate, RetryInfo info) {
            this.delegate = delegate;
            this.retry = info;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return this.delegate.getDisplayName(invocationIndex) + " (Retry " + this.retry + ")";
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return this.delegate.getAdditionalExtensions();
        }
    }
}
