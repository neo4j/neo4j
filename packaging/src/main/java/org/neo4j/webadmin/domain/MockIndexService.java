/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.webadmin.domain;

import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;

/**
 * Used to make it possible to instantiate gremlin-wrapped neo4j databases when
 * using a remote database. This is because indexing is not yet implemented in
 * webadmin for remote databases.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class MockIndexService implements IndexService
{

    public IndexHits<Node> getNodes( String arg0, Object arg1 )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Node getSingleNode( String arg0, Object arg1 )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void index( Node arg0, String arg1, Object arg2 )
    {
        // TODO Auto-generated method stub

    }

    public void removeIndex( String arg0 )
    {
        // TODO Auto-generated method stub

    }

    public void removeIndex( Node arg0, String arg1 )
    {
        // TODO Auto-generated method stub

    }

    public void removeIndex( Node arg0, String arg1, Object arg2 )
    {
        // TODO Auto-generated method stub

    }

    public void shutdown()
    {
        // TODO Auto-generated method stub

    }

}
