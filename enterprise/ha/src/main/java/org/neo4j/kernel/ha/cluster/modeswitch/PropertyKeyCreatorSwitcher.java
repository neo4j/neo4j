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
package org.neo4j.kernel.ha.cluster.modeswitch;

import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.SlavePropertyTokenCreator;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.core.DefaultPropertyTokenCreator;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;

public class PropertyKeyCreatorSwitcher extends AbstractComponentSwitcher<TokenCreator>
{
    private final DelegateInvocationHandler<Master> master;
    private final RequestContextFactory requestContextFactory;
    private final Supplier<Kernel> kernelSupplier;
    private final IdGeneratorFactory idGeneratorFactory;

    public PropertyKeyCreatorSwitcher( DelegateInvocationHandler<TokenCreator> delegate,
            DelegateInvocationHandler<Master> master, RequestContextFactory requestContextFactory,
            Supplier<Kernel> kernelSupplier, IdGeneratorFactory idGeneratorFactory )
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
        return new DefaultPropertyTokenCreator( kernelSupplier, idGeneratorFactory );
    }

    @Override
    protected TokenCreator getSlaveImpl()
    {
        return new SlavePropertyTokenCreator( master.cement(), requestContextFactory );
    }
}
