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
package org.neo4j.procedure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.neo4j.annotations.api.PublicApi;

/**
 * This defines the name of an input argument for a procedure. This is used to determine which values from
 * to use as arguments for the procedure when it is called. For instance, if you are invoking a procedure
 * using parameters, the name you declare here will map to names of the parameters.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
@PublicApi
public @interface Name {
    /**
     * @return the name of this input argument.
     */
    String value();

    /**
     * @return the description of this input argument.
     */
    String description() default "";

    String defaultValue() default DEFAULT_VALUE;

    /*
     * Defaults in annotation requires compile time constants, the only way
     * to check if a returned defaultValue() is a default is to use a constant
     * that is highly unlikely to be used in real code.
     */
    String DEFAULT_VALUE = " <[6795b15e-8693-4a21-b57a-4a7b87f09a5a]> ";
}
