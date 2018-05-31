/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.tooling.procedure;

import java.net.URL;

import javax.annotation.processing.Processor;
import javax.tools.JavaFileObject;

import com.google.testing.compile.CompilationRule;
import com.google.testing.compile.CompileTester;
import com.google.testing.compile.JavaFileObjects;
import org.junit.Rule;
import org.junit.Test;

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
                .in( sproc ).onLine( 39 );
        warningCompilationClause.withWarningContaining(
                "@org.neo4j.procedure.Context usage warning: found unsupported restricted type " +
                "<org.neo4j.server.security.enterprise.log.SecurityLog> on EnterpriseProcedure#securityLog.\n" +
                "  The procedure will not load unless declared via the configuration option 'dbms.security.procedures.unrestricted'.\n" +
                "  You can ignore this warning by passing the option -AIgnoreContextWarnings to the Java compiler" )
                .in( sproc ).onLine( 42 );
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
