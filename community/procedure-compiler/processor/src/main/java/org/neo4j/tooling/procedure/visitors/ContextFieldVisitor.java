/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.tooling.procedure.visitors;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.SimpleElementVisitor8;
import javax.lang.model.util.Types;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.ProcedureTransaction;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.messages.ContextFieldError;
import org.neo4j.tooling.procedure.messages.ContextFieldWarning;

import static org.neo4j.tooling.procedure.CompilerOptions.IGNORE_CONTEXT_WARNINGS_OPTION;

class ContextFieldVisitor extends SimpleElementVisitor8<Stream<CompilationMessage>,Void>
{
    private static final Set<String> SUPPORTED_TYPES = new LinkedHashSet<>(
            Arrays.asList( GraphDatabaseService.class.getName(), Log.class.getName(), TerminationGuard.class.getName(),
                    SecurityContext.class.getName(), ProcedureTransaction.class.getName() ) );
    private static final Set<String> RESTRICTED_TYPES = new LinkedHashSet<>(
            Arrays.asList( GraphDatabaseAPI.class.getName(), KernelTransaction.class.getName(),
                    DependencyResolver.class.getName(), UserManager.class.getName(),
                    // the following classes are not in the compiler classpath
                    "org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager",
                    "org.neo4j.server.security.enterprise.log.SecurityLog" ) );

    private final Elements elements;
    private final Types types;
    private final boolean ignoresWarnings;

    ContextFieldVisitor( Types types, Elements elements, boolean ignoresWarnings )
    {
        this.elements = elements;
        this.types = types;
        this.ignoresWarnings = ignoresWarnings;
    }

    @Override
    public Stream<CompilationMessage> visitVariable( VariableElement field, Void ignored )
    {
        return Stream.concat( validateModifiers( field ), validateInjectedTypes( field ) );
    }

    private Stream<CompilationMessage> validateModifiers( VariableElement field )
    {
        if ( !hasValidModifiers( field ) )
        {
            return Stream.of( new ContextFieldError( field,
                    "@%s usage error: field %s should be public, non-static and non-final", Context.class.getName(),
                    fieldFullName( field ) ) );
        }

        return Stream.empty();
    }

    private Stream<CompilationMessage> validateInjectedTypes( VariableElement field )
    {
        TypeMirror fieldType = field.asType();
        if ( injectsAllowedType( fieldType ) )
        {
            return Stream.empty();
        }

        if ( injectsRestrictedType( fieldType ) )
        {
            if ( ignoresWarnings )
            {
                return Stream.empty();
            }

            return Stream.of( new ContextFieldWarning( field, "@%s usage warning: found unsupported restricted type <%s> on %s.\n" +
                    "The procedure will not load unless declared via the configuration option 'dbms.security.procedures.unrestricted'.\n" +
                    "You can ignore this warning by passing the option -A%s to the Java compiler",
                    Context.class.getName(), fieldType.toString(), fieldFullName( field ),
                    IGNORE_CONTEXT_WARNINGS_OPTION ) );
        }

        return Stream.of( new ContextFieldError( field,
                "@%s usage error: found unknown type <%s> on field %s, expected one of: %s",
                Context.class.getName(), fieldType.toString(), fieldFullName( field ),
                joinTypes( SUPPORTED_TYPES ) ) );
    }

    private boolean injectsAllowedType( TypeMirror fieldType )
    {
        return matches( fieldType, SUPPORTED_TYPES );
    }

    private boolean injectsRestrictedType( TypeMirror fieldType )
    {
        return matches( fieldType, RESTRICTED_TYPES );
    }

    private boolean matches( TypeMirror fieldType, Set<String> typeNames )
    {
        return typeMirrors( typeNames ).anyMatch( t -> types.isSameType( t, fieldType ) );
    }

    private boolean hasValidModifiers( VariableElement field )
    {
        Set<Modifier> modifiers = field.getModifiers();
        return modifiers.contains( Modifier.PUBLIC ) && !modifiers.contains( Modifier.STATIC ) &&
                !modifiers.contains( Modifier.FINAL );
    }

    private Stream<TypeMirror> typeMirrors( Set<String> typeNames )
    {
        return typeNames.stream().map( elements::getTypeElement ).filter( Objects::nonNull ).map( Element::asType );
    }

    private String fieldFullName( VariableElement field )
    {
        return String.format( "%s#%s", field.getEnclosingElement().getSimpleName(), field.getSimpleName() );
    }

    private static String joinTypes( Set<String> types )
    {
        return types.stream().collect( Collectors.joining( ">, <", "<", ">" ) );
    }
}
