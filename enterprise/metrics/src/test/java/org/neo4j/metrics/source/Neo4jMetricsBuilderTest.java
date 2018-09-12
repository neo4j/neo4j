/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.metrics.source;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.impl.util.DependencySatisfier;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.metrics.output.EventReporter;
import org.neo4j.metrics.source.server.ServerMetrics;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.factory.DatabaseInfo.COMMUNITY;

@ExtendWith( TestDirectoryExtension.class )
class Neo4jMetricsBuilderTest
{
    @Inject
    private TestDirectory testDir;

    @Test
    void shouldAddServerMetricsWhenServerEnabled()
    {
        testBuildingWithServerMetrics( true );
    }

    @Test
    void shouldNotAddServerMetricsWhenServerDisabled()
    {
        testBuildingWithServerMetrics( false );
    }

    private void testBuildingWithServerMetrics( boolean serverMetricsEnabled )
    {
        Config config = configWithServerMetrics( serverMetricsEnabled );
        KernelContext kernelContext = new SimpleKernelContext( testDir.databaseDir(), COMMUNITY, mock( DependencySatisfier.class ) );
        LifeSupport life = new LifeSupport();

        Neo4jMetricsBuilder builder = new Neo4jMetricsBuilder( new MetricRegistry(), mock( EventReporter.class ), config, NullLogService.getInstance(),
                kernelContext, mock( Neo4jMetricsBuilder.Dependencies.class ), life );

        assertTrue( builder.build() );

        if ( serverMetricsEnabled )
        {
            assertThat( life.getLifecycleInstances(), hasItem( instanceOf( ServerMetrics.class ) ) );
        }
        else
        {
            assertThat( life.getLifecycleInstances(), not( hasItem( instanceOf( ServerMetrics.class ) ) ) );
        }
    }

    private static Config configWithServerMetrics( boolean enabled )
    {
        return Config.builder()
                .withSetting( new HttpConnector( "http" ).enabled, Boolean.toString( enabled ) )
                .withSetting( MetricsSettings.neoServerEnabled, "true" )
                .build();
    }
}
