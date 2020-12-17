/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.dbms.database;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;

/**
 * The system database is used to store information of interest to many parts of the DBMS. Each sub-graph can be thought of as being of primary interest to (or
 * managed by) a particular component of the DBMS. For example, the security sub-graph composed of User, Role, Privilege and various other related nodes and
 * relationships is primarily initialized, upgraded and read by the SecurityModule.
 * <p>
 * This interface defines an external API to each component whereby external commands can ask about the current status of the component's sub-graph within the
 * system database, and also trigger upgrades (or migrations) of the sub-graph from an older version to the current version.
 * <p>
 * It is up to each component to define the rules for which older versions would be concurrently supported within any running DBMS. However, being supported
 * also means it is possible to upgrade to the current version. This means that if the <code>detect</code> method return <code>REQUIRES_UPGRADE</code>, then it
 * should be possible to call the <code>upgradeToCurrent</code> method and achieve the upgrade.
 */
public interface SystemGraphComponent
{
    Label VERSION_LABEL = Label.label( "Version" );

    /**
     * This string should be a unique identifier for the component. It will be used in two places:
     * <ol>
     *     <li>A key in a map of components for managing upgrades of the entire DBMS</li>
     *     <li>A property key for the <code>Version</code> node used to store versioning information for this component</li>
     * </ol>
     */
    String componentName();

    /**
     * Return the current status of this component. This involves reading the current contents of the system database and detecting whether the sub-graph
     * managed by this component is missing, old or up-to-date. Older sub-graphs could be supported for upgrade, or unsupported (in which case the server cannot
     * be started).
     */
    Status detect( Transaction tx );

    /**
     * If the component-specific sub-graph of the system database is not initialized yet (empty), this method should populate it with the default contents for
     * the current version of the component.
     *
     * @throws Exception on any possible error raised by the initialization process
     */
    void initializeSystemGraph( GraphDatabaseService system, boolean firstInitialization ) throws Exception;

    /**
     * If the component-specific sub-graph of the system database is an older, but still supported, version, this method should upgrade it to the latest
     * supported version.
     *
     * @throws Exception on any possible error raised by the upgrade process
     */
    void upgradeToCurrent( GraphDatabaseService system ) throws Exception;

    static void executeWithFullAccess( GraphDatabaseService system, ThrowingConsumer<Transaction,Exception> consumer ) throws Exception
    {
        try ( TransactionImpl tx = (TransactionImpl) system.beginTx();
              KernelTransaction.Revertable ignore = tx.kernelTransaction().overrideWith( SecurityContext.AUTH_DISABLED ) )
        {
            consumer.accept( tx );
            tx.commit();
        }
    }

    default Integer getVersionNumber( Transaction tx, String componentVersionProperty )
    {
        Integer result = null;
        ResourceIterator<Node> nodes = tx.findNodes( VERSION_LABEL );
        if ( nodes.hasNext() )
        {
            Node versionNode = nodes.next();
            if ( versionNode.hasProperty( componentVersionProperty ) )
            {
                result = (Integer) versionNode.getProperty( componentVersionProperty );
            }
        }
        nodes.close();
        return result;
    }

    enum Status
    {
        UNINITIALIZED( "No sub-graph detected for this component", "requires initialization" ),
        CURRENT( "The sub-graph is already at the current version", "nothing to do" ),
        REQUIRES_UPGRADE( "The sub-graph is supported, but is an older version and requires upgrade", "CALL dbms.upgrade()" ),
        UNSUPPORTED_BUT_CAN_UPGRADE( "The sub-graph is unsupported, this component cannot function",
                "Restart Neo4j in single-instance mode to upgrade this component at startup" ),
        UNSUPPORTED( "The sub-graph is unsupported because it is too old, this component cannot function",
                "Downgrade Neo4j and then upgrade this component before upgrading Neo4j again" ),
        UNSUPPORTED_FUTURE( "The sub-graph is unsupported because it is a newer version, this component cannot function", "Upgrade Neo4j" );

        private final String description;
        private final String resolution;

        Status( String description, String resolution )
        {
            this.description = description;
            this.resolution = resolution;
        }

        /**
         * Combine status's to present an overall status for the entire DBMS. Primarily we care about two cases:
         * <ul>
         *     <li>If any component is unsupported, the entire DBMS is unsupported</li>
         *     <li>If any component requires an upgrade, the entire DBMS requires an upgrade</li>
         * </ul>
         * Initialization is handled through a different code path and so does not require handling here.
         */
        public Status with( Status other )
        {
            Status[] precedence = new Status[]{UNSUPPORTED_FUTURE, UNSUPPORTED, UNSUPPORTED_BUT_CAN_UPGRADE, REQUIRES_UPGRADE, UNINITIALIZED, CURRENT};
            for ( Status status : precedence )
            {
                if ( other == status || this == status )
                {
                    return status;
                }
            }
            return this;
        }

        public String description()
        {
            return description;
        }

        public String resolution()
        {
            return resolution;
        }
    }
}
