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
package org.neo4j.bolt.test.connection.setup;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Optional;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.test.connection.resolver.property.MutableTestPropertyContext;

@FunctionalInterface
public interface SettingCustomizer {

    void customize(Context ctx, SettingBuilder settings);

    class Context {
        private final ExtensionContext ctx;
        private final MutableTestPropertyContext properties;

        private final Class<?> targetClass;
        private final Method targetMethod;

        public Context(
                ExtensionContext ctx,
                MutableTestPropertyContext properties,
                Class<?> targetClass,
                Method targetMethod) {
            this.ctx = ctx;
            this.properties = properties;

            this.targetClass = targetClass;
            this.targetMethod = targetMethod;
        }

        public ExtensionContext extension() {
            return this.ctx;
        }

        public MutableTestPropertyContext properties() {
            return this.properties;
        }

        public Optional<Method> targetMethod() {
            return Optional.ofNullable(this.targetMethod);
        }

        public Class<?> targetClass() {
            return this.targetClass;
        }

        public <A extends Annotation> Optional<A> findAnnotation(Class<A> annotationType) {
            return Optional.ofNullable(this.targetMethod()
                    .map(method -> method.getAnnotation(annotationType))
                    .orElseGet(() -> this.targetClass.getAnnotation(annotationType)));
        }
    }
}
