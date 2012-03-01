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
package org.neo4j.kernel.impl.core;

import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

public class TestLockReleaser extends AbstractNeo4jTestCase
{
    /**
     * Here we test a memory leak as reported on 26/4/2011 by dmontag Removing a
     * non existing property (even if added in the same tx) causes an entry to
     * be added in this primitive's COW map in the lock releaser only to not be
     * used later on. This entry is not removed if the property remove is the
     * only operation because the tx is treated read only and no cleanup is
     * performed.
     */
    @Test
    public void removeNonExistentPropertyDoesNotLeak()
    {
        Node n = getGraphDb().createNode();
        // Remove a non existing property
        n.removeProperty( "dummy" );
        // Retrieve the primitive, necessary for getting its cow property map
        // below
        NodeImpl primitive = getEmbeddedGraphDb().getNodeManager().getNodeForProxy( n.getId(), null );
        /*
         *  The cow property remove map should be null (i.e. not created in the first place )
         *  since the property did not exist
         */
        assertNull( getEmbeddedGraphDb().getLockReleaser().getCowPropertyRemoveMap(
                primitive ) );
        getTransaction().finish();
    }
}
