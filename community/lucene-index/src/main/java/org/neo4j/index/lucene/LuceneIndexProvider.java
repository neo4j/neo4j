/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.index.lucene;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.index.impl.lucene.LuceneIndexImplementation;

/**
 * This has been replace by the Lucene Kernel Extension
 */
@Deprecated
public class LuceneIndexProvider extends IndexProvider
{
    public LuceneIndexProvider( )
    {
        super( LuceneIndexImplementation.SERVICE_NAME );
    }

    @Override
    public IndexImplementation load( DependencyResolver dependencyResolver)
    {
        // No-op. The kernel extension will do what this class used to do
        return null;
    }
}
