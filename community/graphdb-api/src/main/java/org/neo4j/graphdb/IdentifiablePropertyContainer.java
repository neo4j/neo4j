/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.graphdb;

public interface IdentifiablePropertyContainer extends PropertyContainer {

    /**
     * Returns the unique id of this {@link PropertyContainer}. Id's are garbage
     * collected over time so they are only guaranteed to be unique during a
     * specific time span: if the {@link PropertyContainer} is deleted, it's
     * likely that a new {@link PropertyContainer} at some point will get the old
     * id. <b>Note</b>: this makes {@link PropertyContainer} id's brittle as
     * public APIs.
     *
     * @return The id of this {@link PropertyContainer}
     */
     long getId();

}
