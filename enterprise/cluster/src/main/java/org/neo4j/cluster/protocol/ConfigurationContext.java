/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cluster.protocol;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.ClusterInstanceId;

public interface ConfigurationContext
{
    ClusterInstanceId getMyId();

    List<URI> getMemberURIs();

    URI boundAt();

    List<URI> getAcceptors();

    Map<ClusterInstanceId,URI> getMembers();

    ClusterInstanceId getCoordinator();

    URI getUriForId( ClusterInstanceId id );

    ClusterInstanceId getIdForUri(URI uri);

    boolean isMe( ClusterInstanceId server );
}
