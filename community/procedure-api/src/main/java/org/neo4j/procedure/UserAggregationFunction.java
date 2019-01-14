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
package org.neo4j.procedure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a method as an aggregation function, meaning the method can be called from the
 * cypher query language.
 * <p>
 * An aggregation function must returned an instance of a class with two annotated methods, one annotated with
 * {@link UserAggregationUpdate} and one annotated with {@link UserAggregationResult}
 *
 * <h2>Resource declarations</h2>
 * The aggregation function method itself can contain arbitrary Java code - but in order to work with the underlying graph,
 * it must have access to the graph API. This is done by declaring fields in the function class, and annotating
 * them with the {@link Context} annotation. Fields declared this way are automatically injected with the
 * requested resource. This is how functions gain access to APIs to do work with.
 * <p>
 * All fields in the class containing the function declaration must either be static; or it must be public, non-final
 * and annotated with {@link Context}.
 * <p>
 * Resources supported by default are as follows:
 * <ul>
 *     <li>{@link org.neo4j.graphdb.GraphDatabaseService}</li>
 *     <li>{@link org.neo4j.logging.Log}</li>
 *     <li>{@link org.neo4j.procedure.TerminationGuard}</li>
 * </ul>
 *
 * <h2>Lifecycle and state</h2>
 * The class that declares your function method may be re-instantiated before each call. Because of this,
 * no regular state can be stored in the fields of the function.
 * <p>
 * If you want to maintain state between invocations to your function, simply use a static field. Note that
 * functions may be called concurrently, meaning you need to take care to ensure the state you store in
 * static fields can be safely accessed by multiple callers simultaneously.
 */
@Target( ElementType.METHOD )
@Retention( RetentionPolicy.RUNTIME )
public @interface UserAggregationFunction
{
    /**
     * The namespace and name for the aggregation function, as a period-separated
     * string. For instance {@code myfunctions.myfunction}.
     *
     * If this is left empty, the name defaults to the package name of
     * the class the procedure is declared in, combined with the method
     * name. Notably, the class name is omitted.
     *
     * @return the namespace and procedure name
     */
    String value() default "";

    /**
     * Synonym for {@link #value()}
     * @return the namespace and procedure name.
     */
    String name() default "";

    /**
     * When deprecating a function it is useful to indicate a possible
     * replacement procedure that clients might show in warnings.
     * @return a string representation of the replacement procedure.
     */
    String deprecatedBy() default "";
}
