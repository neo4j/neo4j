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
package org.neo4j.tooling.procedure.visitors;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVisitor;
import javax.lang.model.util.Elements;
import javax.lang.model.util.SimpleElementVisitor8;
import javax.lang.model.util.Types;
import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.ReturnTypeError;

public class ProcedureVisitor extends SimpleElementVisitor8<Stream<CompilationMessage>, Void> {

    private final Types typeUtils;
    private final Elements elementUtils;
    private final ElementVisitor<Stream<CompilationMessage>, Void> classVisitor;
    private final TypeVisitor<Stream<CompilationMessage>, Void> recordVisitor;
    private final ElementVisitor<Stream<CompilationMessage>, Void> parameterVisitor;
    private final Collection<TypeMirror> invalidStreamTypeParameters;

    public ProcedureVisitor(Types typeUtils, Elements elementUtils, boolean ignoresWarnings) {
        TypeMirrorUtils typeMirrors = new TypeMirrorUtils(typeUtils, elementUtils);

        this.typeUtils = typeUtils;
        this.elementUtils = elementUtils;
        this.classVisitor = new ExtensionClassVisitor(typeUtils, elementUtils, ignoresWarnings);
        this.recordVisitor = new RecordTypeVisitor(typeUtils, typeMirrors);
        this.parameterVisitor = new ParameterVisitor(new ParameterTypeVisitor(typeUtils, typeMirrors));
        this.invalidStreamTypeParameters = typeMirrors.procedureAllowedTypes().stream()
                .map(typeUtils::erasure) // convert e.g. Map<K,V> to Map
                .collect(Collectors.toSet());
    }

    /**
     * Validates method parameters and return type
     */
    @Override
    public Stream<CompilationMessage> visitExecutable(ExecutableElement executableElement, Void ignored) {
        return Stream.of(
                        classVisitor.visit(executableElement.getEnclosingElement()),
                        validateParameters(executableElement.getParameters()),
                        validateReturnType(executableElement))
                .flatMap(Function.identity());
    }

    private Stream<CompilationMessage> validateParameters(List<? extends VariableElement> parameters) {
        return parameters.stream().flatMap(parameterVisitor::visit);
    }

    private TypeMirror getStreamTypeParameter(TypeMirror returnType) {
        var typeParameters = ((DeclaredType) returnType).getTypeArguments();
        assert (typeParameters.size() == 1); // Stream<T> must only have one parameter type
        return typeParameters.get(0);
    }

    private Stream<CompilationMessage> hintInvalidStreamType(ExecutableElement method, TypeMirror returnType) {
        var typeParameter = getStreamTypeParameter(returnType);
        var erasedTypeParameter = typeUtils.erasure(typeParameter);

        // Note: this is not an exhaustive check of all invalid type parameters
        // but rather a means to give users a hint on how to correct an error that
        // is easy to make.
        if (!invalidStreamTypeParameters.contains(erasedTypeParameter)) {
            return Stream.empty();
        }

        return Stream.of(new ReturnTypeError(
                method,
                "Return type of %s#%s must be %s<T> where T is a custom class per procedure, but was %s",
                method.getEnclosingElement().getSimpleName(),
                method.getSimpleName(),
                Stream.class.getCanonicalName(),
                typeParameter));
    }

    private Stream<CompilationMessage> validateReturnType(ExecutableElement method) {
        String streamClassName = Stream.class.getCanonicalName();

        TypeMirror streamType =
                typeUtils.erasure(elementUtils.getTypeElement(streamClassName).asType());
        TypeMirror returnType = method.getReturnType();
        TypeMirror erasedReturnType = typeUtils.erasure(returnType);

        TypeMirror voidType = typeUtils.getNoType(TypeKind.VOID);
        if (typeUtils.isSameType(returnType, voidType)) {
            return Stream.empty();
        }

        if (!typeUtils.isSubtype(erasedReturnType, streamType)) {
            return Stream.of(new ReturnTypeError(
                    method,
                    "Return type of %s#%s must be %s<T>",
                    method.getEnclosingElement().getSimpleName(),
                    method.getSimpleName(),
                    streamClassName));
        }

        var hints = hintInvalidStreamType(method, returnType).collect(Collectors.toList());
        if (!hints.isEmpty()) {
            return hints.stream();
        }

        return recordVisitor.visit(returnType);
    }
}
