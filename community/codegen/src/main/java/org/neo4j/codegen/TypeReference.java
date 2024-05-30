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
package org.neo4j.codegen;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.neo4j.util.Preconditions;
import org.neo4j.values.AnyValue;

public class TypeReference {
    public static Bound extending(Class<?> type) {
        return extending(typeReference(type));
    }

    public static Bound extending(final TypeReference type) {
        return new Bound(type) {
            @Override
            public TypeReference extendsBound() {
                return type;
            }

            @Override
            public TypeReference superBound() {
                return null;
            }
        };
    }

    private static TypeReference primitiveType(Class<?> base) {
        return new TypeReference("", base.getSimpleName(), 0, false, null, base.getModifiers());
    }

    private static TypeReference primitiveArray(Class<?> base, int arrayDepth) {
        assert base.isPrimitive();

        return new TypeReference("", base.getSimpleName(), arrayDepth, false, null, base.getModifiers());
    }

    public static TypeReference typeReference(Class<?> type) {
        if (type == void.class) {
            return VOID;
        }
        if (type == Object.class) {
            return OBJECT;
        }

        Class<?> innerType = type;
        int arrayDepth = 0;
        while (innerType.isArray()) {
            innerType = innerType.getComponentType();
            arrayDepth++;
        }

        if (innerType.isPrimitive()) {
            return arrayDepth > 0 ? primitiveArray(innerType, arrayDepth) : primitiveType(innerType);
        } else {
            String packageName = "";
            String name;
            TypeReference declaringTypeReference = null;
            Package typePackage = innerType.getPackage();
            if (typePackage != null) {
                packageName = typePackage.getName();
            }
            Class<?> declaringClass = innerType.getDeclaringClass();
            if (declaringClass != null) {
                declaringTypeReference = typeReference(declaringClass);
            }
            name = innerType.getSimpleName();
            return new TypeReference(packageName, name, arrayDepth, false, declaringTypeReference, type.getModifiers());
        }
    }

    public static TypeReference typeParameter(String name) {
        return new TypeReference("", name, 0, true, null, Modifier.PUBLIC);
    }

    public static TypeReference fullTypeReference(
            String packageName,
            String name,
            int arrayDepth,
            boolean isTypeParameter,
            TypeReference declaringClass,
            int modifiers,
            TypeReference... parameters) {
        return new TypeReference(packageName, name, arrayDepth, isTypeParameter, declaringClass, modifiers, parameters);
    }

    public static TypeReference arrayOf(TypeReference type) {
        return new TypeReference(
                type.packageName, type.name, type.arrayDepth + 1, false, type.declaringClass, type.modifiers);
    }

    public static TypeReference parameterizedType(Class<?> base, Class<?>... parameters) {
        return parameterizedType(typeReference(base), typeReferences(parameters));
    }

    public static TypeReference parameterizedType(Class<?> base, TypeReference... parameters) {
        return parameterizedType(typeReference(base), parameters);
    }

    public static TypeReference parameterizedType(TypeReference base, TypeReference... parameters) {
        return new TypeReference(
                base.packageName, base.name, base.arrayDepth, false, base.declaringClass, base.modifiers, parameters);
    }

    public static TypeReference[] typeReferences(Class<?> first, Class<?>[] more) {
        TypeReference[] result = new TypeReference[more.length + 1];
        result[0] = typeReference(first);
        for (int i = 0; i < more.length; i++) {
            result[i + 1] = typeReference(more[i]);
        }
        return result;
    }

    public static TypeReference[] typeReferences(Class<?>[] types) {
        TypeReference[] result = new TypeReference[types.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = typeReference(types[i]);
        }
        return result;
    }

    public static TypeReference toBoxedType(TypeReference in) {
        switch (in.fullName()) {
            case "byte":
                return TypeReference.typeReference(Byte.class);
            case "short":
                return TypeReference.typeReference(Short.class);
            case "int":
                return TypeReference.typeReference(Integer.class);
            case "long":
                return TypeReference.typeReference(Long.class);
            case "char":
                return TypeReference.typeReference(Character.class);
            case "boolean":
                return TypeReference.typeReference(Boolean.class);
            case "float":
                return TypeReference.typeReference(Float.class);
            case "double":
                return TypeReference.typeReference(Double.class);
            default:
                return in;
        }
    }

    public static TypeReference toUnboxedType(TypeReference in) {
        if (in.isPrimitive()) {
            return in;
        }

        switch (in.fullName()) {
            case "java.lang.Byte":
                return TypeReference.typeReference(byte.class);
            case "java.lang.Short":
                return TypeReference.typeReference(short.class);
            case "java.lang.Integer":
                return TypeReference.typeReference(int.class);
            case "java.lang.Long":
                return TypeReference.typeReference(long.class);
            case "java.lang.Character":
                return TypeReference.typeReference(char.class);
            case "java.lang.Boolean":
                return TypeReference.typeReference(boolean.class);
            case "java.lang.Float":
                return TypeReference.typeReference(float.class);
            case "java.lang.Double":
                return TypeReference.typeReference(double.class);
            default:
                throw new IllegalStateException("Cannot unbox " + in.fullName());
        }
    }

    private final String packageName;
    private final String name;
    private final TypeReference[] parameters;
    private final int arrayDepth;
    private final boolean isTypeParameter;
    private final TypeReference declaringClass;
    private final int modifiers;

    public static final TypeReference VOID = new TypeReference("", "void", 0, false, null, void.class.getModifiers());
    public static final TypeReference OBJECT =
            new TypeReference("java.lang", "Object", 0, false, null, Object.class.getModifiers());
    public static final TypeReference BOOLEAN =
            new TypeReference("", "boolean", 0, false, null, boolean.class.getModifiers());
    public static final TypeReference INT = new TypeReference("", "int", 0, false, null, int.class.getModifiers());
    public static final TypeReference LONG = new TypeReference("", "long", 0, false, null, long.class.getModifiers());
    public static final TypeReference DOUBLE =
            new TypeReference("", "double", 0, false, null, double.class.getModifiers());
    public static final TypeReference BOOLEAN_ARRAY =
            new TypeReference("", "boolean", 1, false, null, boolean.class.getModifiers());
    public static final TypeReference INT_ARRAY =
            new TypeReference("", "int", 1, false, null, int.class.getModifiers());
    public static final TypeReference LONG_ARRAY =
            new TypeReference("", "long", 1, false, null, long.class.getModifiers());
    public static final TypeReference DOUBLE_ARRAY =
            new TypeReference("", "double", 1, false, null, double.class.getModifiers());
    public static final TypeReference VALUE =
            new TypeReference("org.neo4j.values", "AnyValue", 0, false, null, AnyValue.class.getModifiers());
    static final TypeReference[] NO_TYPES = new TypeReference[0];

    TypeReference(
            String packageName,
            String name,
            int arrayDepth,
            boolean isTypeParameter,
            TypeReference declaringClass,
            int modifiers,
            TypeReference... parameters) {
        this.packageName = packageName;
        this.name = name;
        this.arrayDepth = arrayDepth;
        this.isTypeParameter = isTypeParameter;
        this.declaringClass = declaringClass;
        this.modifiers = modifiers;
        this.parameters = parameters;
    }

    public String packageName() {
        return packageName;
    }

    public String name() {
        return name;
    }

    public String simpleName() {
        StringBuilder builder = new StringBuilder(name);
        builder.append("[]".repeat(Math.max(0, arrayDepth)));
        return builder.toString();
    }

    public boolean isPrimitive() {
        if (isArray()) {
            return false;
        } else {
            switch (name) {
                case "int":
                case "byte":
                case "short":
                case "char":
                case "boolean":
                case "long":
                case "float":
                case "double":
                    return true;
                default:
                    return false;
            }
        }
    }

    boolean isTypeParameter() {
        return isTypeParameter;
    }

    public boolean isGeneric() {
        return parameters == null || parameters.length > 0;
    }

    public List<TypeReference> parameters() {
        return List.of(parameters);
    }

    public String fullName() {
        return writeTo(new StringBuilder()).toString();
    }

    public boolean isArray() {
        return arrayDepth > 0;
    }

    public TypeReference elementOfArray() {
        Preconditions.checkState(isArray(), "Should only be called on array");
        return new TypeReference(
                packageName, name, arrayDepth - 1, isTypeParameter, declaringClass, modifiers, parameters);
    }

    public int arrayDepth() {
        return arrayDepth;
    }

    public boolean isVoid() {
        return this == VOID;
    }

    public boolean isInnerClass() {
        return declaringClass != null;
    }

    List<TypeReference> declaringClasses() {
        LinkedList<TypeReference> parents = new LinkedList<>();
        TypeReference parent = declaringClass;
        while (parent != null) {
            parents.addFirst(parent);
            parent = parent.declaringClass;
        }
        return parents;
    }

    public int modifiers() {
        return modifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TypeReference reference = (TypeReference) o;

        if (arrayDepth != reference.arrayDepth) {
            return false;
        }
        if (isTypeParameter != reference.isTypeParameter) {
            return false;
        }
        if (modifiers != reference.modifiers) {
            return false;
        }
        if (!Objects.equals(packageName, reference.packageName)) {
            return false;
        }
        if (!Objects.equals(name, reference.name)) {
            return false;
        }
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(parameters, reference.parameters)) {
            return false;
        }
        return Objects.equals(declaringClass, reference.declaringClass);
    }

    @Override
    public int hashCode() {
        int result = packageName != null ? packageName.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(parameters);
        result = 31 * result + arrayDepth;
        result = 31 * result + (isTypeParameter ? 1 : 0);
        result = 31 * result + (declaringClass != null ? declaringClass.hashCode() : 0);
        result = 31 * result + modifiers;
        return result;
    }

    public String baseName() {
        return writeBaseType(new StringBuilder()).toString();
    }

    @Override
    public String toString() {
        return writeTo(new StringBuilder().append("TypeReference[")).append(']').toString();
    }

    StringBuilder writeTo(StringBuilder result) {
        writeBaseType(result);
        result.append("[]".repeat(Math.max(0, arrayDepth)));
        if (!(parameters == null || parameters.length == 0)) {
            result.append('<');
            String sep = "";
            for (TypeReference parameter : parameters) {
                parameter.writeTo(result.append(sep));
                sep = ",";
            }
            result.append('>');
        }
        return result;
    }

    private StringBuilder writeBaseType(StringBuilder result) {
        if (!packageName.isEmpty()) {
            result.append(packageName).append('.');
        }
        List<TypeReference> parents = declaringClasses();
        for (TypeReference parent : parents) {
            result.append(parent.name).append('.');
        }
        result.append(name);
        return result;
    }

    public abstract static class Bound {
        private final TypeReference type;

        private Bound(TypeReference type) {
            this.type = type;
        }

        public abstract TypeReference extendsBound();

        public abstract TypeReference superBound();
    }
}
