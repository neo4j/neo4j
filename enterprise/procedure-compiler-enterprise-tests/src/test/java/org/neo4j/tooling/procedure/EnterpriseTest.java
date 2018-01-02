/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tooling.procedure;

import com.google.testing.compile.CompilationRule;
import com.google.testing.compile.CompileTester;
import com.google.testing.compile.JavaFileObjects;
import org.junit.Rule;
import org.junit.Test;

import java.net.URL;
import javax.annotation.processing.Processor;
import javax.tools.JavaFileObject;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

public class EnterpriseTest
{

    @Rule
    public CompilationRule compilation = new CompilationRule();

    private Processor processor = new ProcedureProcessor();

    @Test
    public void emits_warnings_for_restricted_enterprise_types()
    {

        JavaFileObject sproc =
                JavaFileObjects.forResource( resolveUrl( "context/restricted_types/EnterpriseProcedure.java" ) );

        CompileTester.SuccessfulCompilationClause warningCompilationClause =
                assert_().about( javaSource() ).that( sproc ).processedWith( processor ).compilesWithoutError()
                        .withWarningCount( 3 );
        warningCompilationClause.withWarningContaining(
                "@org.neo4j.procedure.Context usage warning: found unsupported restricted type " +
                "<org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager> on EnterpriseProcedure#enterpriseAuthManager.\n" +
                "  The procedure will not load unless declared via the configuration option 'dbms.security.procedures.unrestricted'.\n" +
                "  You can ignore this warning by passing the option -AIgnoreContextWarnings to the Java compiler" )
                .in( sproc ).onLine( 36 );
        warningCompilationClause.withWarningContaining(
                "@org.neo4j.procedure.Context usage warning: found unsupported restricted type " +
                "<org.neo4j.server.security.enterprise.log.SecurityLog> on EnterpriseProcedure#securityLog.\n" +
                "  The procedure will not load unless declared via the configuration option 'dbms.security.procedures.unrestricted'.\n" +
                "  You can ignore this warning by passing the option -AIgnoreContextWarnings to the Java compiler" )
                .in( sproc ).onLine( 39 );
    }

    @Test
    public void does_not_emit_warnings_for_restricted_enterprise_types_when_warnings_are_disabled()
    {

        JavaFileObject sproc =
                JavaFileObjects.forResource( resolveUrl( "context/restricted_types/EnterpriseProcedure.java" ) );

        assert_().about( javaSource() ).that( sproc ).withCompilerOptions( "-AIgnoreContextWarnings" )
                .processedWith( processor ).compilesWithoutError().withWarningCount( 1 );
    }

    private URL resolveUrl( String relativePath )
    {
        return this.getClass().getResource( "/org/neo4j/tooling/procedure/procedures/" + relativePath );
    }
}
