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
