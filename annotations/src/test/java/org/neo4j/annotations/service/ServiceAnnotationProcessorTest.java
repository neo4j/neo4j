/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.annotations.service;

import org.junit.jupiter.api.Test;

import static com.google.testing.compile.JavaFileObjects.forResource;
import static com.google.testing.compile.JavaSourcesSubject.assertThat;
import static javax.tools.StandardLocation.CLASS_OUTPUT;

class ServiceAnnotationProcessorTest
{
    @Test
    void providersWithDistinctServiceKeys()
    {
        assertThat(
                forResource( "test/service/FooService.java" ),
                forResource( "test/service/AbstractFooService.java" ),
                forResource( "test/service/FooServiceImplA.java" ),
                forResource( "test/service/FooServiceImplB.java" ) )
                .processedWith( new ServiceAnnotationProcessor() )
                .compilesWithoutError()
                .and()
                .generatesFileNamed( CLASS_OUTPUT, "", "META-INF/services/test.service.FooService" )
                .and()
                .generatesFiles(
                        forResource( "META-INF/services/test.service.FooService" )
                );
    }

    @Test
    void classIsBothServiceAndProvider()
    {
        assertThat(
                forResource( "test/service/ClassIsServiceAndProvider.java" ) )
                .processedWith( new ServiceAnnotationProcessor() )
                .compilesWithoutError()
                .and()
                .generatesFileNamed( CLASS_OUTPUT, "", "META-INF/services/test.service.ClassIsServiceAndProvider" )
                .and()
                .generatesFiles(
                        forResource( "META-INF/services/test.service.ClassIsServiceAndProvider" )
                );
    }

    @Test
    void nestedTypes()
    {
        assertThat(
                forResource( "test/service/Nested.java" ) )
                .processedWith( new ServiceAnnotationProcessor() )
                .compilesWithoutError()
                .and()
                .generatesFileNamed( CLASS_OUTPUT, "", "META-INF/services/test.service.Nested$NestedService" )
                .and()
                .generatesFiles(
                        forResource( "META-INF/services/test.service.Nested$NestedService" )
                );
    }

    @Test
    void failMultipleServiceAscendants()
    {
        assertThat(
                forResource( "test/service/FailMultipleServices.java" ) )
                .processedWith( new ServiceAnnotationProcessor() )
                .failsToCompile()
                .withErrorContaining( "multiple ascendants annotated with @Service" );
    }

    @Test
    void failProviderDoesntImplementService()
    {
        assertThat(
                forResource( "test/service/FailNotService.java" ) )
                .processedWith( new ServiceAnnotationProcessor() )
                .failsToCompile()
                .withErrorContaining( "neither has ascendants nor itself annotated with @Service" );
    }
}
