/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cluster.protocol.omega;

import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.cluster.protocol.omega.state.View;

public class CollectionRound
{
    private final Map<URI, View> oldViews;
    private final Set<URI> responders;

    private final int collectionRound;

    public CollectionRound( Map<URI, View> oldViews, int collectionRound )
    {
        this.oldViews = oldViews;
        this.collectionRound = collectionRound;
        this.responders = new HashSet<URI>();
    }

    public Map<URI, View> getOldViews()
    {
        return oldViews;
    }

    public void responseReceived( URI from )
    {
        responders.add( from );
    }

    public Iterable<URI> getResponders()
    {
        return responders;
    }

    public int getResponseCount()
    {
        return  responders.size();
    }
}
