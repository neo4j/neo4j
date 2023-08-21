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
package org.neo4j.bolt.test.extension.lifecycle;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.test.extension.db.ServerInstanceContext;

public class ServerInstanceManager implements BeforeEachCallback, AfterEachCallback {

    private final ServerInstanceContext context;

    public ServerInstanceManager(ServerInstanceContext context) {
        this.context = context;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        var server = this.context.configure(context);
        server.init(new BoltTestInfo(context));
    }

    @Override
    public void afterEach(ExtensionContext context) {
        this.context.stop(context);
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
