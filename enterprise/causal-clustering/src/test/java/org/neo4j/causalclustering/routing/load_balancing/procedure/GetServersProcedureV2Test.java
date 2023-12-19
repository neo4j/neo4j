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
package org.neo4j.causalclustering.routing.load_balancing.procedure;

import org.junit.Test;

import java.util.Map;

import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;

public class GetServersProcedureV2Test
{
    @Test
    public void shouldHaveCorrectSignature()
    {
        // given
        GetServersProcedureForMultiDC proc = new GetServersProcedureForMultiDC( null );

        // when
        ProcedureSignature signature = proc.signature();

        // then
        assertThat( signature.inputSignature(), containsInAnyOrder(
                FieldSignature.inputField( "context", NTMap ) ) );

        assertThat( signature.outputSignature(), containsInAnyOrder(
                FieldSignature.outputField( "ttl", NTInteger ),
                FieldSignature.outputField( "servers", NTList( NTMap ) ) ) );
    }

    @Test
    public void shouldPassClientContextToPlugin() throws Exception
    {
        // given
        LoadBalancingPlugin plugin = mock( LoadBalancingPlugin.class );
        LoadBalancingProcessor.Result result = mock( LoadBalancingPlugin.Result.class );
        when( plugin.run( anyMap() ) ).thenReturn( result );
        GetServersProcedureForMultiDC getServers = new GetServersProcedureForMultiDC( plugin );
        Map<String,String> clientContext = stringMap( "key", "value", "key2", "value2" );

        // when
        getServers.apply( null, new Object[]{clientContext}, null );

        // then
        verify( plugin ).run( clientContext );
    }
}
