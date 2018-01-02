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
package org.neo4j.kernel.ha.cluster.modeswitch;

import java.util.function.Supplier;

import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.SlaveRelationshipTypeCreator;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.core.TokenCreator;

public class RelationshipTypeCreatorSwitcher extends AbstractComponentSwitcher<TokenCreator>
{
    private final DelegateInvocationHandler<Master> master;
    private final RequestContextFactory requestContextFactory;
    private final Supplier<InwardKernel> kernelSupplier;
    private final IdGeneratorFactory idGeneratorFactory;

    public RelationshipTypeCreatorSwitcher( DelegateInvocationHandler<TokenCreator> delegate,
            DelegateInvocationHandler<Master> master, RequestContextFactory requestContextFactory,
            Supplier<InwardKernel> kernelSupplier, IdGeneratorFactory idGeneratorFactory )
    {
        super( delegate );
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.kernelSupplier = kernelSupplier;
        this.idGeneratorFactory = idGeneratorFactory;
    }

    @Override
    protected TokenCreator getMasterImpl()
    {
        return new DefaultRelationshipTypeCreator( kernelSupplier, idGeneratorFactory );
    }

    @Override
    protected TokenCreator getSlaveImpl()
    {
        return new SlaveRelationshipTypeCreator( master.cement(), requestContextFactory );
    }
}
