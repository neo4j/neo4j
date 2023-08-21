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
package org.neo4j.cli;

import java.lang.reflect.Constructor;
import picocli.CommandLine;

/**
 * This is an extension of the default factory that knows how to construct components with {@link ExecutionContext}.
 * If this factory encounters a component that has a constructor with a single argument of {@link ExecutionContext} type,
 * it will construct it, otherwise it delegates to the default factory.
 */
public class ContextInjectingFactory implements picocli.CommandLine.IFactory {

    private final ExecutionContext ctx;

    public ContextInjectingFactory(ExecutionContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public <K> K create(Class<K> cls) throws Exception {
        Constructor<?>[] constructors = cls.getConstructors();
        for (Constructor<?> constructor : constructors) {
            Class<?>[] parameterTypes = constructor.getParameterTypes();
            if (parameterTypes.length == 1 && parameterTypes[0].isAssignableFrom(ctx.getClass())) {
                return (K) constructor.newInstance(ctx);
            }
        }

        return CommandLine.defaultFactory().create(cls);
    }
}
