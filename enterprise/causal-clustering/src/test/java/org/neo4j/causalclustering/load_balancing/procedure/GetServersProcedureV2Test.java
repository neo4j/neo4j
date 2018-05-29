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
package org.neo4j.causalclustering.load_balancing.procedure;

import org.junit.Test;

import java.util.Map;

import org.neo4j.causalclustering.load_balancing.LoadBalancingPlugin;
import org.neo4j.kernel.api.proc.FieldSignature;
import org.neo4j.kernel.api.proc.ProcedureSignature;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTInteger;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTList;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTMap;

public class GetServersProcedureV2Test
{
    @Test
    public void shouldHaveCorrectSignature() throws Exception
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
        when( plugin.run( anyMap() ) ).thenReturn( mock( LoadBalancingPlugin.Result.class ) );
        GetServersProcedureForMultiDC getServers = new GetServersProcedureForMultiDC( plugin );
        Map<String,String> clientContext = stringMap( "key", "value", "key2", "value2" );

        // when
        getServers.apply( null, new Object[]{clientContext}, null );

        // then
        verify( plugin ).run( clientContext );
    }
}
