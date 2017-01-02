/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import com.google.testing.compile.CompilationRule;
import org.neo4j.tooling.procedure.testutils.JavaFileObjectUtils;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.processing.Processor;
import javax.tools.JavaFileObject;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

public class PerformsWriteProcessorTest
{
    @Rule
    public CompilationRule compilation = new CompilationRule();

    private Processor processor = new PerformsWriteProcessor();

    @Test
    public void fails_with_conflicting_mode()
    {
        JavaFileObject procedure = JavaFileObjectUtils.INSTANCE.procedureSource(
                "invalid/conflicting_mode/ConflictingMode.java" );

        assert_().about( javaSource() ).that( procedure ).processedWith( processor ).failsToCompile()
                .withErrorCount( 1 )
                .withErrorContaining( "@PerformsWrites usage error: cannot use mode other than Mode.DEFAULT" )
                .in( procedure ).onLine( 30 );

    }
}
