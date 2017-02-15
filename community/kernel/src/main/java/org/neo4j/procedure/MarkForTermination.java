/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.procedure;

import org.neo4j.kernel.api.exceptions.Status;

/**
 * MarkForTermination allows to mark a transaction for termination, this will make it so that it can not commit.
 */
public interface MarkForTermination
{

    /**
     * Marks this transaction for termination, such that it cannot commit successfully and will try to be
     * terminated by having other methods throw a specific termination exception.
     */
    void mark(Status status);
}
