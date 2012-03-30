/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.kernel.ha;

import ch.qos.logback.classic.LoggerContext;
import java.util.Map;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.util.StringLogger;
import org.slf4j.impl.StaticLoggerBinder;

/**
 * TODO
 */
public class AbstractHAGraphDatabase
    extends AbstractGraphDatabase
{
    protected Broker broker;
    private StringLogger logger;
    private NodeProxy.NodeLookup nodeLookup;
    private RelationshipProxy.RelationshipLookups relationshipLookups;
    private HighlyAvailableGraphDatabase highlyAvailableGraphDatabase;

    public AbstractHAGraphDatabase( String storeDir, Map<String, String> params,
                                    HighlyAvailableGraphDatabase highlyAvailableGraphDatabase,
                                    Broker broker, StringLogger logger,
                                    NodeProxy.NodeLookup nodeLookup,
                                    RelationshipProxy.RelationshipLookups relationshipLookups,
                                    Iterable<IndexProvider> indexProviders1, Iterable<KernelExtension> kernelExtensions
    )
    {
        super( storeDir, params, indexProviders1, kernelExtensions );
        this.highlyAvailableGraphDatabase = highlyAvailableGraphDatabase;

        assert broker != null && logger != null && nodeLookup != null && relationshipLookups != null;

        this.broker = broker;
        this.logger = logger;
        this.nodeLookup = nodeLookup;
        this.relationshipLookups = relationshipLookups;
    }

    @Override
    protected KernelData createKernelData()
    {
        return new DefaultKernelData(config, this);
    }

    @Override
    protected NodeProxy.NodeLookup createNodeLookup()
    {
        return nodeLookup;
    }

    @Override
    protected RelationshipProxy.RelationshipLookups createRelationshipLookups()
    {
        return relationshipLookups;
    }

    @Override
    protected StringLogger createStringLogger()
    {
        loggerContext = (LoggerContext) StaticLoggerBinder.getSingleton().getLoggerFactory();
        return logger;
    }

    public HighlyAvailableGraphDatabase getHighlyAvailableGraphDatabase()
    {
        return highlyAvailableGraphDatabase;
    }
}
