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
 * Declares a method as the update method of an aggregation.
 * <p>
 * The update method is called multiple times and allows the class to aggregate a result later retrieved from the
 * method
 * annotated with {@link UserAggregationResult}.
 *
 * <h2>Input declaration</h2>
 * The update method can accept input arguments, which is defined in the arguments to the
 * annotated method. Each method argument must be a valid input type, and
 * each must be annotated with the {@link Name} annotation, declaring the input name.
 * <p>
 * Valid input types are as follows:
 * <ul>
 * <li>{@link String}</li>
 * <li>{@link Long} or {@code long}</li>
 * <li>{@link Double} or {@code double}</li>
 * <li>{@link Number}</li>
 * <li>{@link Boolean} or {@code boolean}</li>
 * <li>{@link org.neo4j.graphdb.Node}</li>
 * <li>{@link org.neo4j.graphdb.Relationship}</li>
 * <li>{@link org.neo4j.graphdb.Path}</li>
 * <li>{@link java.util.Map} with key {@link String} and value of any type in this list, including {@link
 * java.util.Map}</li>
 * <li>{@link java.util.List} with element type of any type in this list, including {@link java.util.List}</li>
 * <li>{@link Object}, meaning any of the valid input types above</li>
 * </ul>
 *
 * The update method cannot return any value and must be a void method.
 */
@Target( ElementType.METHOD )
@Retention( RetentionPolicy.RUNTIME )
public @interface UserAggregationUpdate
{
}
