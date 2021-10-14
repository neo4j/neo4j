/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.dbms.systemgraph.versions;


import org.neo4j.dbms.database.ComponentVersion;
import org.neo4j.dbms.database.KnownSystemComponentVersion;
import org.neo4j.dbms.database.SystemGraphDbmsModel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;

import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_ACCESS_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_LABEL;

public abstract class KnownCommunityTopologyComponentVersion extends KnownSystemComponentVersion
{
    KnownCommunityTopologyComponentVersion( ComponentVersion componentVersion )
    {
        // TODO we do not have access to the security log here should we send in the neo4j log instead?
        super( componentVersion, CommunitySecurityLog.NULL_LOG );
    }

    /**
     * Initialize the topology graph to this version.
     *
     * @param tx open transaction to perform the initialization in
     */
    public void initializeTopologyGraph( Transaction tx )
    {
        setDatabaseAccessToReadWrite( tx );
    }

    /**
     * Set access property to read-write for all existing nodes
     *
     * @param tx open transaction to perform the method in
     */
    protected void setDatabaseAccessToReadWrite( Transaction tx )
    {
        final ResourceIterator<Node> nodes = tx.findNodes( DATABASE_LABEL );
        nodes.stream().forEach( node -> node.setProperty( DATABASE_ACCESS_PROPERTY, SystemGraphDbmsModel.DatabaseAccess.READ_WRITE.toString() ) );
    }

    /**
     * Upgrade the topology graph to this version.
     * This method recursively calls older versions and performs the upgrades in steps.
     *
     * @param tx open transaction to perform the upgrade in
     * @param fromVersion the detected version, upgrade will be performed rolling from this
     */
    public abstract void upgradeTopologyGraph( Transaction tx, int fromVersion ) throws Exception;
}
