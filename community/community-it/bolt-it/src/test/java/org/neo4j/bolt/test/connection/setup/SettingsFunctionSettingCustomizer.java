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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.extension.TestInstantiationException;

public class SettingsFunctionSettingCustomizer implements SettingCustomizer {

    @Override
    public void customize(Context ctx, SettingBuilder settings) {
        var method = ctx.targetMethod()
                // this should never actually occur since the annotation targets are constraint
                // appropriately, but we're providing a sensible message anyway just to be sure
                .orElseThrow(() -> new IllegalStateException(
                        "@SettingsFunction annotation must be placed on a method within your test class"));

        method.setAccessible(true);

        var params = method.getParameterTypes();
        if (params.length != 1 || !params[0].isAssignableFrom(SettingBuilder.class)) {
            throw new TestInstantiationException(
                    "@SettingsFunction annotation must have exactly one parameter of type SettingBuilder");
        }

        MethodHandle handle;
        try {
            handle = MethodHandles.lookup().unreflect(method);
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException("Cannot access @SettingsFunction " + method.getName(), ex);
        }

        if (!Modifier.isStatic(method.getModifiers())) {
            handle = handle.bindTo(ctx.extension().getRequiredTestInstance());
        }

        try {
            handle.invoke(settings);
        } catch (Throwable ex) {
            throw new TestInstantiationException("Failed to invoke @SettingsFunction", ex);
        }
    }
}
