/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.stresstests.transaction.checkpoint.mutation;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

/**
 * Created by davide on 22/06/15.
 */
class PropertyMutation implements Mutation
{
    private final GraphDatabaseService db;

    public PropertyMutation( GraphDatabaseService db )
    {
        this.db = db;
    }

    @Override
    public void perform( long nodeId, String value )
    {
        Node node = db.getNodeById( nodeId );
        if ( node.hasProperty( value ) )
        {
            node.removeProperty( value );
        }
        else
        {
            node.setProperty( value, 10 );
        }
    }
}
