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

package org.neo4j.kernel;

import java.util.Collection;
import javax.transaction.TransactionManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.persistence.PersistenceSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;

/**
 * This SPI can be used by internals to get access to services.
 *
 * TODO: This should go away. It indicates lack of abstractions somewhere. DO NOT ADD MORE USAGE OF THIS!
 */
@Deprecated
public interface GraphDatabaseSPI
    extends GraphDatabaseService
{
    NodeManager getNodeManager();

    LockReleaser getLockReleaser();

    LockManager getLockManager();

    XaDataSourceManager getXaDataSourceManager();

    TransactionManager getTxManager();

    DiagnosticsManager getDiagnosticsManager();
    
    StringLogger getMessageLog();

    RelationshipTypeHolder getRelationshipTypeHolder();

    IdGeneratorFactory getIdGeneratorFactory();

    String getStoreDir();

    KernelData getKernelData();

    <T> T getSingleManagementBean( Class<T> type );

    TransactionBuilder tx();
    
    PersistenceSource getPersistenceSource();

    <T> Collection<T> getManagementBeans( Class<T> type );
    
    KernelPanicEventGenerator getKernelPanicGenerator();

    Guard getGuard();
}
