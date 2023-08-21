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
package org.neo4j.annotations.api;

import static com.google.testing.compile.JavaFileObjects.forResource;
import static com.google.testing.compile.JavaSourcesSubject.assertThat;
import static javax.tools.StandardLocation.CLASS_OUTPUT;
import static org.neo4j.annotations.AnnotationTestHelper.detectNewLineSignature;
import static org.neo4j.annotations.api.PublicApiAnnotationProcessor.GENERATED_SIGNATURE_DESTINATION;
import static org.neo4j.annotations.api.PublicApiAnnotationProcessor.VERIFY_TOGGLE;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PublicApiAnnotationProcessorTest {
    @BeforeEach
    void setUp() {
        System.setProperty(VERIFY_TOGGLE, "true");
    }

    @Test
    void printPublicSignature() throws Exception {
        assertThat(forResource("org/neo4j/annotations/api/PublicClass.java"))
                .processedWith(
                        new PublicApiAnnotationProcessor(true, detectNewLineSignature("signatures/PublicClass.txt")))
                .compilesWithoutError()
                .and()
                .generatesFileNamed(CLASS_OUTPUT, "", GENERATED_SIGNATURE_DESTINATION)
                .and()
                .generatesFiles(forResource("signatures/PublicClass.txt"));
    }

    @Test
    void printAnnotationSignature() throws Exception {
        assertThat(forResource("org/neo4j/annotations/api/PublicAnnotation.java"))
                .processedWith(new PublicApiAnnotationProcessor(
                        true, detectNewLineSignature("signatures/PublicAnnotation.txt")))
                .compilesWithoutError()
                .and()
                .generatesFileNamed(CLASS_OUTPUT, "", GENERATED_SIGNATURE_DESTINATION)
                .and()
                .generatesFiles(forResource("signatures/PublicAnnotation.txt"));
    }

    @Test
    void failClassIsNotPublic() {
        assertThat(forResource("org/neo4j/annotations/api/NonPublicClass.java"))
                .processedWith(new PublicApiAnnotationProcessor(true))
                .failsToCompile()
                .withErrorContaining("Class marked as public is not actually public");
    }

    @Test
    void failClassIsNotMarkedPublic() {
        assertThat(
                        forResource("org/neo4j/annotations/api/ExposingPublicInterface.java"),
                        forResource("org/neo4j/annotations/api/NotMarkedClass.java"))
                .processedWith(new PublicApiAnnotationProcessor(true))
                .failsToCompile()
                .withErrorContaining("Error processing org.neo4j.annotations.api.ExposingPublicInterface::i():"
                        + System.lineSeparator()
                        + "  org.neo4j.annotations.api.NotMarkedClass exposed through the API is not marked with @"
                        + PublicApi.class.getSimpleName());
    }

    @Test
    void failNestedClassIsNotMarkedPublic() {
        assertThat(
                        forResource("org/neo4j/annotations/api/ExposingPublicNestedInterface.java"),
                        forResource("org/neo4j/annotations/api/NotMarkedNestedClass.java"))
                .processedWith(new PublicApiAnnotationProcessor(true))
                .failsToCompile()
                .withErrorContaining(
                        "Error processing org.neo4j.annotations.api.ExposingPublicNestedInterface::nested():"
                                + System.lineSeparator()
                                + "  org.neo4j.annotations.api.NotMarkedNestedClass.Nested's parent, "
                                + "org.neo4j.annotations.api.NotMarkedNestedClass, is not marked with @"
                                + PublicApi.class.getSimpleName());
    }

    @Test
    void successDeprecatedClassIsNotMarkedPublic() throws Exception {
        assertThat(
                        forResource("org/neo4j/annotations/api/ExposingDeprecatedPublicInterface.java"),
                        forResource("org/neo4j/annotations/api/NotMarkedClass.java"))
                .processedWith(new PublicApiAnnotationProcessor(
                        true, detectNewLineSignature("signatures/ExposingDeprecatedPublicInterface.txt")))
                .compilesWithoutError()
                .and()
                .generatesFileNamed(CLASS_OUTPUT, "", GENERATED_SIGNATURE_DESTINATION)
                .and()
                .generatesFiles(forResource("signatures/ExposingDeprecatedPublicInterface.txt"))
                .withWarningContaining(
                        "Non-public element, org.neo4j.annotations.api.NotMarkedNestedClass, is exposed through the API via a deprecated method");
    }
}
