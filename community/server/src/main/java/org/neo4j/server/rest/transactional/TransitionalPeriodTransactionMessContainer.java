/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.rest.transactional;

import javax.servlet.http.HttpServletRequest;

import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContext;
import org.neo4j.kernel.impl.query.QuerySession;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.server.rest.web.ServerQuerySession;

public class TransitionalPeriodTransactionMessContainer
{
    private static final PropertyContainerLocker locker = new PropertyContainerLocker();

    private final GraphDatabaseFacade db;
    private final ThreadToStatementContextBridge txBridge;

    public TransitionalPeriodTransactionMessContainer( GraphDatabaseFacade db )
    {
        this.db = db;
        this.txBridge = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
    }

    public TransitionalTxManagementKernelTransaction newTransaction( Type type, AccessMode mode )
    {
        return new TransitionalTxManagementKernelTransaction( db, type, mode, txBridge );
    }

    public ThreadToStatementContextBridge getBridge()
    {
        return txBridge;
    }

    public QuerySession create(  GraphDatabaseQueryService service, Type type, AccessMode mode, HttpServletRequest request )
    {
        InternalTransaction transaction = db.beginTransaction( type, mode );
        TransactionalContext context = new Neo4jTransactionalContext( service, transaction, txBridge.get(), locker );
        return new ServerQuerySession( request, context );
    }
}
