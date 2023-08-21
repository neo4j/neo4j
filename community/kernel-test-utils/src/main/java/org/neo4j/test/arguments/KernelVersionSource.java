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
package org.neo4j.test.arguments;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.neo4j.kernel.KernelVersion;

/**
 * {@code KernelVersionSource} is an {@link ArgumentsSource} for
 * parameterized test that provides {@link KernelVersion}s.
 *
 * <p>The versions can be filtered with the available parameters:
 * <ul>
 *     <li>{@link #lessThan()}: only includes versions strictly
 *     less than the provided version.
 *     <li>{@link #greaterThan()}: only includes versions strictly
 *     greater than the provided version.
 *     <li>{@link #atLeast()}: only includes versions that is equal
 *     to the provided version or greater.
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 *     @ParameterizedTest
 *     @KernelVersionSource(lessThan = "5.0")
 *     void myTest(KernelVersion kernelVersion) {
 *         // All versions before 5.0
 *     }
 * }</pre>
 *
 * <p>The {@link #lessThan()} filter can be combined with
 * {@link #greaterThan()} or {@link #atLeast()}. They can act as
 * both an intersection or disjunction, which ever is applicable.
 *
 * <p>Intersection:
 * <pre>{@code
 *     @ParameterizedTest
 *     @KernelVersionSource(atLeast = "4.0", lessThan = "4.2")
 *     void myTest(KernelVersion kernelVersion) {
 *         // Version 4.0 or 4.1
 *     }
 * }</pre>
 *
 * <p>Disjunction:
 * <pre>{@code
 *     @ParameterizedTest
 *     @KernelVersionSource(lessThan = "4.0", greaterThan = "4.0")
 *     void myTest(KernelVersion kernelVersion) {
 *         // All versions except 4.0
 *     }
 * }</pre>
 *
 * @see KernelVersion
 * @see ParameterizedTest
 */
@Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ArgumentsSource(KernelVersionArgumentsProvider.class)
public @interface KernelVersionSource {
    String lessThan() default "";

    String greaterThan() default "";

    String atLeast() default "";
}
