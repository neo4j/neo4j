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
package org.neo4j.test.extension.actors;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.neo4j.test.extension.Inject;

/**
 * This JUnit 5 extension enables the {@link Inject injection} of {@link Actor} fields.
 * <p>
 * Multiple actor fields can be injected, and each field will have its own independent actor, named after the field.
 * <p>
 * This makes it easy to write tests that use multiple threads.
 */
@Target( {ElementType.TYPE} )
@Retention( RetentionPolicy.RUNTIME )
@ExtendWith( {ActorsSupportExtension.class} )
public @interface ActorsExtension
{
}
