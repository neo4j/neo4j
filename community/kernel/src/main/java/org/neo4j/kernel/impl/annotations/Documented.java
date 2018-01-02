/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines documentation for a class, interface, field or method.
 *
 * If no documentation is given for the {@link #value() value} to this
 * annotation, the JavaDoc documentation comment will be
 * {@link DocumentationProcessor extracted at compile time} and inserted as the
 * {@link #value() value} of this annotation. If no JavaDoc is specified a
 * compiler warning will be issued.
 *
 * Note that for the JavaDoc to be possible to be extracted it must come before
 * any annotation on the documented element.
 *
 * @author Tobias Ivarsson
 */
@Retention( RetentionPolicy.RUNTIME )
@Target( { ElementType.TYPE, ElementType.FIELD, ElementType.METHOD } )
public @interface Documented
{
    String value();
}
