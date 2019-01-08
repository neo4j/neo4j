/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
