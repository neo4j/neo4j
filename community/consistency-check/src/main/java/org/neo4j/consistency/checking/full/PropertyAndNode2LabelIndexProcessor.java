/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.checking.ChainCheck;
import org.neo4j.consistency.checking.PropertyRecordCheck;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.full.MandatoryProperties.Check;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.function.Function;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

/**
 * Processor of node records with the context of how they're indexed.
 */
public class PropertyAndNode2LabelIndexProcessor extends RecordProcessor.Adapter<NodeRecord>
{
    private final ConsistencyReporter reporter;
    private final RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeIndexCheck;
    private final RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> propertyCheck;
    private final CacheAccess cacheAccess;
    private final Function<NodeRecord,Check<NodeRecord,NodeConsistencyReport>> mandatoryProperties;

    public PropertyAndNode2LabelIndexProcessor( ConsistencyReporter reporter,
            IndexAccessors indexes,
            PropertyReader propertyReader,
            CacheAccess cacheAccess,
            Function<NodeRecord,MandatoryProperties.Check<NodeRecord,ConsistencyReport.NodeConsistencyReport>> mandatoryProperties )
    {
        this.reporter = reporter;
        this.cacheAccess = cacheAccess;
        this.mandatoryProperties = mandatoryProperties;
        this.nodeIndexCheck = new PropertyAndNodeIndexedCheck( indexes, propertyReader, cacheAccess );
        this.propertyCheck = new PropertyRecordCheck();
    }

    @Override
    public void process( NodeRecord nodeRecord )
    {
        reporter.forNode( nodeRecord, nodeIndexCheck );
        CacheAccess.Client client = cacheAccess.client();
        try ( MandatoryProperties.Check<NodeRecord,ConsistencyReport.NodeConsistencyReport> mandatoryCheck =
                mandatoryProperties.apply( nodeRecord ) )
        {
            Iterable<PropertyRecord> properties = client.getPropertiesFromCache();

            // We do this null-check here because even if nodeIndexCheck should provide the properties for us,
            // or an empty list at least, it may fail in one way or another and exception be caught by
            // broad exception handler in reporter. The caught exception will produce an ERROR so it will not
            // go by unnoticed.
            if ( properties != null )
            {
                for ( PropertyRecord property : properties )
                {
                    reporter.forProperty( property, propertyCheck );
                    mandatoryCheck.receive( ChainCheck.keys( property ) );
                }
            }
        }
    }
}
