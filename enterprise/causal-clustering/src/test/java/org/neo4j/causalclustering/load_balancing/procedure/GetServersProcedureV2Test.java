/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.load_balancing.procedure;

import org.junit.Test;

import java.util.Map;

import org.neo4j.causalclustering.load_balancing.LoadBalancingStrategy;
import org.neo4j.kernel.api.proc.FieldSignature;
import org.neo4j.kernel.api.proc.ProcedureSignature;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTInteger;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTMap;

public class GetServersProcedureV2Test
{
    @Test
    public void shouldHaveCorrectSignature() throws Exception
    {
        // given
        GetServersProcedureV2 proc = new GetServersProcedureV2( null );

        // when
        ProcedureSignature signature = proc.signature();

        // then
        assertThat( signature.inputSignature(), containsInAnyOrder(
                new FieldSignature( "context", NTMap ) ) );

        assertThat( signature.outputSignature(), containsInAnyOrder(
                new FieldSignature( "ttl", NTInteger ),
                new FieldSignature( "servers", NTMap ) ) );
    }

    @Test
    public void shouldPassClientContextToStrategy() throws Exception
    {
        // given
        LoadBalancingStrategy strategy = mock( LoadBalancingStrategy.class );
        when( strategy.run( anyMap() ) ).thenReturn( mock( LoadBalancingStrategy.Result.class ) );
        GetServersProcedureV2 getServers = new GetServersProcedureV2( strategy );
        Map<String,String> clientContext = stringMap( "key", "value", "key2", "value2" );

        // when
        getServers.apply( null, new Object[] { clientContext } );

        // then
        verify( strategy ).run( clientContext );
    }
}
