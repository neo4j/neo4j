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
package org.neo4j.bolt.test.extension.lifecycle;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketSupportExtension;

public class ServerInstanceManager implements BeforeEachCallback, AfterEachCallback {

    private final BiConsumer<ExtensionContext, Neo4jWithSocket> serverInitializer;

    public ServerInstanceManager(BiConsumer<ExtensionContext, Neo4jWithSocket> serverInitializer) {
        this.serverInitializer = serverInitializer;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        var server = Neo4jWithSocketSupportExtension.getInstance(context);
        this.serverInitializer.accept(context, server);

        server.init(new BoltTestInfo(context));
    }

    @Override
    public void afterEach(ExtensionContext context) {
        var server = Neo4jWithSocketSupportExtension.getInstance(context);
        server.shutdownDatabase();
    }

    private static class BoltTestInfo implements TestInfo {
        private final String displayName;
        private final Set<String> tags;
        private final Optional<Class<?>> testClass;
        private final Optional<Method> testMethod;

        private BoltTestInfo(ExtensionContext context) {
            this.displayName = context.getDisplayName();
            this.tags = context.getTags();
            this.testClass = context.getTestClass();
            this.testMethod = context.getTestMethod();
        }

        @Override
        public String getDisplayName() {
            return this.displayName;
        }

        @Override
        public Set<String> getTags() {
            return this.tags;
        }

        @Override
        public Optional<Class<?>> getTestClass() {
            return this.testClass;
        }

        @Override
        public Optional<Method> getTestMethod() {
            return this.testMethod;
        }
    }
}
