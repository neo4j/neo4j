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
package org.neo4j.kernel;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;

/**
 * An implementation of {@link GraphDatabaseService} that is used to embed Neo4j
 * in an application. You typically instantiate it by using
 * {@link org.neo4j.graphdb.factory.GraphDatabaseFactory} like so:
 * <p/>
 *
 * <pre>
 * <code>
 * GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( &quot;var/graphdb&quot; );
 * // ... use Neo4j
 * graphDb.shutdown();
 * </code>
 * </pre>
 * <p/>
 * For more information, see {@link GraphDatabaseService}.
 *
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public class EmbeddedGraphDatabase extends InternalAbstractGraphDatabase
{
    /**
     * Internal constructor used by {@link org.neo4j.graphdb.factory.GraphDatabaseFactory}
     */
    public EmbeddedGraphDatabase( String storeDir,
                                  Map<String, String> params,
                                  Dependencies dependencies )
    {
        super( storeDir, params, dependencies );

        run();
    }
}
