/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.annotations.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to mark types that are part of Neo4j public API and indicate that marked classes, interfaces, enums are intended to be used by external consumers.<br/>
 * <li>Types that are not explicitly marked with {@code PublicApi}
 * should be considered <b>private</b> and <b>not covered</b> by semantic versioning compatibility contracts.</li>
 * <p>
 * <li>Any external applications must only use types that are explicitly marked with {@code PublicApi}</li>
 */
@Target( {ElementType.TYPE} )
@Retention( RetentionPolicy.RUNTIME )
public @interface PublicApi
{
}
