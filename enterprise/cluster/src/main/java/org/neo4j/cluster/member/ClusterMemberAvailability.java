/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
