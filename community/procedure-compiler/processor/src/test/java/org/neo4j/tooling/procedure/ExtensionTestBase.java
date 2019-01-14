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
package org.neo4j.tooling.procedure;

import com.google.testing.compile.CompileTester;
import org.junit.Test;

import javax.annotation.processing.Processor;
import javax.tools.JavaFileObject;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.logging.Log;
import org.neo4j.procedure.ProcedureTransaction;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.tooling.procedure.testutils.JavaFileObjectUtils;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

public abstract class ExtensionTestBase
{

    abstract Processor processor();

    @Test
    public void fails_if_context_injected_fields_have_wrong_modifiers()
    {
        JavaFileObject sproc =
                JavaFileObjectUtils.INSTANCE.procedureSource( "invalid/bad_context_field/BadContextFields.java" );

        CompileTester.UnsuccessfulCompilationClause unsuccessfulCompilationClause =
                assert_().about( javaSource() ).that( sproc ).processedWith( processor() ).failsToCompile()
                        .withErrorCount( 4 );

        unsuccessfulCompilationClause.withErrorContaining(
                "@org.neo4j.procedure.Context usage error: field BadContextFields#shouldBeNonStatic should be public, " +
                "non-static and non-final" )
                .in( sproc ).onLine( 35 );

        unsuccessfulCompilationClause.withErrorContaining(
                "@org.neo4j.procedure.Context usage error: field BadContextFields#shouldBeNonFinal should be public, " +
                "non-static and non-final" )
                .in( sproc ).onLine( 38 );

        unsuccessfulCompilationClause.withErrorContaining(
                "@org.neo4j.procedure.Context usage error: field BadContextFields#shouldBePublic should be public, " +
                "non-static and non-final" )
                .in( sproc ).onLine( 42 );

        unsuccessfulCompilationClause.withErrorContaining( "Field BadContextFields#shouldBeStatic should be static" )
                .in( sproc ).onLine( 43 );
    }

    @Test
    public void emits_warnings_if_context_injected_field_types_are_restricted()
    {
        JavaFileObject sproc = JavaFileObjectUtils.INSTANCE
                .procedureSource( "invalid/bad_context_field/BadContextRestrictedTypeField.java" );

        assert_().about( javaSource() ).that( sproc ).processedWith( processor() ).compilesWithoutError()
                .withWarningCount( 2 ).withWarningContaining(
                "@org.neo4j.procedure.Context usage warning: found unsupported restricted type <org.neo4j.kernel.internal" +
                ".GraphDatabaseAPI> on BadContextRestrictedTypeField#notOfficiallySupported.\n" +
                "  The procedure will not load unless declared via the configuration option 'dbms.security.procedures.unrestricted'.\n" +
                "  You can ignore this warning by passing the option -AIgnoreContextWarnings to the Java compiler" )
                .in( sproc ).onLine( 35 );
    }

    @Test
    public void does_not_emit_warnings_if_context_injected_field_types_are_restricted_when_context_warnings_disabled()
    {
        JavaFileObject sproc = JavaFileObjectUtils.INSTANCE
                .procedureSource( "invalid/bad_context_field/BadContextRestrictedTypeField.java" );

        assert_().about( javaSource() ).that( sproc ).withCompilerOptions( "-AIgnoreContextWarnings" )
                .processedWith( processor() ).compilesWithoutError().withWarningCount( 1 );
    }

    @Test
    public void fails_if_context_injected_fields_have_unsupported_types()
    {
        JavaFileObject sproc = JavaFileObjectUtils.INSTANCE
                .procedureSource( "invalid/bad_context_field/BadContextUnsupportedTypeError.java" );

        assert_().about( javaSource() ).that( sproc ).processedWith( processor() ).failsToCompile().withErrorCount( 1 )
                .withErrorContaining(
                        "@org.neo4j.procedure.Context usage error: found unknown type <java.lang.String> on field " +
                                "BadContextUnsupportedTypeError#foo, expected one of: <" +
                                GraphDatabaseService.class.getName() + ">, <" +
                                Log.class.getName() + ">, <" +
                                TerminationGuard.class.getName() + ">, <" +
                                SecurityContext.class.getName() + ">, <" +
                                ProcedureTransaction.class.getName() + ">" )
                .in( sproc ).onLine( 33 );
    }
}
