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
package org.neo4j.annotations.service;

import static java.lang.annotation.ElementType.TYPE;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.neo4j.annotations.api.PublicApi;

/**
 * Used to mark a type as a service as defined in {@link java.util.ServiceLoader}.
 * Subtypes of such service type, which are service providers loaded via service loading mechanism,
 * should be annotated with {@link ServiceProvider}.
 * <p/>
 * Processed by {@link ServiceAnnotationProcessor}.
 */
@Target(TYPE)
@Retention(RetentionPolicy.CLASS)
@PublicApi
public @interface Service {}
