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
                forResource( "org/neo4j/annotations/service/FooService.java" ),
                forResource( "org/neo4j/annotations/service/AbstractFooService.java" ),
                forResource( "org/neo4j/annotations/service/FooServiceImplA.java" ),
                forResource( "org/neo4j/annotations/service/FooServiceImplB.java" ) )
                .processedWith( new ServiceAnnotationProcessor() )
                .compilesWithoutError()
                .and()
                .generatesFileNamed( CLASS_OUTPUT, "", "META-INF/services/org.neo4j.annotations.service.FooService" )
                .and()
                .generatesFiles(
                        forResource( "META-INF/services/org.neo4j.annotations.service.FooService" )
                );
    }

    @Test
    void classIsBothServiceAndProvider()
    {
        assertThat(
                forResource( "org/neo4j/annotations/service/ClassIsServiceAndProvider.java" ) )
                .processedWith( new ServiceAnnotationProcessor() )
                .compilesWithoutError()
                .and()
                .generatesFileNamed( CLASS_OUTPUT, "", "META-INF/services/org.neo4j.annotations.service.ClassIsServiceAndProvider" )
                .and()
                .generatesFiles(
                        forResource( "META-INF/services/org.neo4j.annotations.service.ClassIsServiceAndProvider" )
                );
    }

    @Test
    void nestedTypes()
    {
        assertThat(
                forResource( "org/neo4j/annotations/service/Nested.java" ) )
                .processedWith( new ServiceAnnotationProcessor() )
                .compilesWithoutError()
                .and()
                .generatesFileNamed( CLASS_OUTPUT, "", "META-INF/services/org.neo4j.annotations.service.Nested$NestedService" )
                .and()
                .generatesFiles(
                        forResource( "META-INF/services/org.neo4j.annotations.service.Nested$NestedService" )
                );
    }

    @Test
    void failMultipleServiceAscendants()
    {
        assertThat(
                forResource( "org/neo4j/annotations/service/FailMultipleServices.java" ) )
                .processedWith( new ServiceAnnotationProcessor() )
                .failsToCompile()
                .withErrorContaining( "multiple ascendants annotated with @Service" );
    }

    @Test
    void failProviderDoesntImplementService()
    {
        assertThat(
                forResource( "org/neo4j/annotations/service/FailNotService.java" ) )
                .processedWith( new ServiceAnnotationProcessor() )
                .failsToCompile()
                .withErrorContaining( "neither has ascendants nor itself annotated with @Service" );
    }
}
