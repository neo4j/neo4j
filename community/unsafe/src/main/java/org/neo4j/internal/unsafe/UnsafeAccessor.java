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
package org.neo4j.internal.unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import sun.misc.Unsafe;

final class UnsafeAccessor {
    private UnsafeAccessor() {}

    static Unsafe getUnsafe() {
        try {
            return Unsafe.getUnsafe();
        } catch (Exception e) {
            Class<Unsafe> type = Unsafe.class;
            try {
                Field[] fields = type.getDeclaredFields();
                for (Field field : fields) {
                    if (Modifier.isStatic(field.getModifiers()) && type.isAssignableFrom(field.getType())) {
                        field.setAccessible(true);
                        return type.cast(field.get(null));
                    }
                }
            } catch (IllegalAccessException iae) {
                e.addSuppressed(iae);
            }
            var error = new LinkageError("Cannot access sun.misc.Unsafe", e);
            error.addSuppressed(e);
            throw error;
        }
    }
}
