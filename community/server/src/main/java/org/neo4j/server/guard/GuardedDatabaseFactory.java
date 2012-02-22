/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.guard;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.database.GraphDatabaseFactory;

public class GuardedDatabaseFactory implements GraphDatabaseFactory {

    private final GraphDatabaseFactory dbFactory;
    private final Guard guard;

    public GuardedDatabaseFactory(GraphDatabaseFactory dbFactory, final Guard guard) {
        this.dbFactory = dbFactory;
        this.guard = guard;
    }

    @Override
    public AbstractGraphDatabase createDatabase(String databaseStoreDirectory, Map<String, String> databaseProperties) {
        final GraphDatabaseService db = dbFactory.createDatabase(databaseStoreDirectory, databaseProperties);

//        return new WrappedGraphDatabase(db) {
//            // private int cnt = 0;
//
//            @Override protected WrappedNode<WrappedGraphDatabase> node(final Node node, boolean created) {
//                // if (cnt++ % 1000 == 0)
//                guard.check();
//                if (node == null) {
//                    return null;
//                }
//                return new WrappedNode<WrappedGraphDatabase>(this) {
//                    @Override protected Node actual() {
//                        return node;
//                    }
//                };
//            }
//
//            @Override
//            protected WrappedRelationship<WrappedGraphDatabase> relationship(final Relationship relationship, boolean created) {
//                // if (cnt++ % 1000 == 0)
//                guard.check();
//                if (relationship == null) {
//                    return null;
//                }
//                return new WrappedRelationship<WrappedGraphDatabase>(this) {
//                    @Override protected Relationship actual() {
//                        return relationship;
//                    }
//                };
//            }
//        };
        throw new RuntimeException( "Need to be implemented" );
    }
}
