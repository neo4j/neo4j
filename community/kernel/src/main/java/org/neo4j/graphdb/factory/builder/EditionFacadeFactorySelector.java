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
package org.neo4j.graphdb.factory.builder;

import java.util.List;
import java.util.NoSuchElementException;

import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

public class EditionFacadeFactorySelector implements GraphDatabaseFacadeFactorySelector
{
    private Edition edition;

    public EditionFacadeFactorySelector(Edition edition)
    {
        this.edition = edition;
    }

    @Override
    public GraphDatabaseFacadeFactory select( List<GraphDatabaseFacadeFactory> factories )
    {
       return factories.stream()
                       .filter( factory -> this.edition == factory.databaseInfo().edition )
                       .findFirst()
                       .orElseThrow( () -> new NoSuchElementException("requested edition not found") );
    }
}
