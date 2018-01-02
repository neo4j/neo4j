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
package org.neo4j.cluster.member;

import java.net.URI;

import org.neo4j.kernel.impl.store.StoreId;

/**
 * This can be used to signal that a cluster member can now actively
 * participate with a given role, accompanied by a URI for accessing that role.
 */
public interface ClusterMemberAvailability
{
    /**
     * When a member has finished a transition to a particular role, i.e. master or slave,
     * then it should call this which will broadcast the new status to the cluster.
     *
     * @param role
     */
    void memberIsAvailable( String role, URI roleUri, StoreId storeId );

    /**
     * When a member is no longer available in a particular role it should call this
     * to announce it to the other members of the cluster.
     *
     * @param role
     */
    void memberIsUnavailable( String role );
}
