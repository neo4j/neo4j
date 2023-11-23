/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util;

import static org.teavm.metaprogramming.Metaprogramming.emit;
import static org.teavm.metaprogramming.Metaprogramming.exit;
import static org.teavm.metaprogramming.Metaprogramming.findClass;
import static org.teavm.metaprogramming.Metaprogramming.unsupportedCase;

import java.util.Arrays;
import org.teavm.metaprogramming.CompileTime;
import org.teavm.metaprogramming.Meta;
import org.teavm.metaprogramming.ReflectClass;
import org.teavm.metaprogramming.Value;
import org.teavm.metaprogramming.reflect.ReflectMethod;
import scala.None;
import scala.Product;
import scala.Some;
import scala.util.Left;
import scala.util.Right;

@CompileTime
public class RewritableJavascript {
    static boolean isSubclassOf(ReflectClass<Object> cls, Class<?> other) {
        var reflectClass = findClass(other);
        return reflectClass.isAssignableFrom(cls);
    }

    public static boolean inAllowList(ReflectClass<Object> cls) {
        return isSubclassOf(cls, ASTNode.class)
                || isSubclassOf(cls, Some.class)
                || isSubclassOf(cls, None.class)
                || isSubclassOf(cls, Left.class)
                || isSubclassOf(cls, Right.class);
    }

    private static ReflectMethod getCopyConstructor(ReflectClass<Object> cls) {
        ReflectMethod method = Arrays.stream(cls.getMethods())
                .filter(m -> m.getName().equals("copy"))
                .findFirst()
                .orElse(null);

        return method;
    }

    public static Object copyProduct(Product product, Object[] children) {
        return copyConstructor(product.getClass(), product, children);
    }

    public int numParameters(Product product) {
        return numParameters(product.getClass());
    }

    public boolean includesPosition(Product product) {
        return lastParamIsPosition(product.getClass());
    }

    @Meta
    public static native Object copyConstructor(Class<?> cls, Object object, Object[] children);

    public static void copyConstructor(ReflectClass<Object> cls, Value<Object> object, Value<Object[]> children) {
        if (!inAllowList(cls)) {
            unsupportedCase();
            return;
        }
        ReflectMethod method = getCopyConstructor(cls);
        Value<Object> result = emit(() -> method.invoke(object.get(), children.get()));
        exit(() -> result.get());
    }

    @Meta
    public static native int numParameters(Class<?> cls);

    public static void numParameters(ReflectClass<Object> cls) {
        if (!inAllowList(cls)) {
            unsupportedCase();
            return;
        }
        ReflectMethod method = getCopyConstructor(cls);
        int result = method.getParameterTypes().length;
        exit(() -> result);
    }

    @Meta
    public static native boolean lastParamIsPosition(Class<?> cls);

    public static void lastParamIsPosition(ReflectClass<Object> cls) {
        if (!inAllowList(cls)) {
            unsupportedCase();
            return;
        }
        ReflectMethod method = getCopyConstructor(cls);
        ReflectClass<?>[] paramTypes = method.getParameterTypes();
        ReflectClass<?> lastParam = paramTypes[paramTypes.length - 1];
        boolean result = lastParam.isAssignableFrom(InputPosition.class);
        exit(() -> result);
    }
}
