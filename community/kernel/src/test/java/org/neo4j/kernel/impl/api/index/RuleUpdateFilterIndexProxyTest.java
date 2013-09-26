/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

public class RuleUpdateFilterIndexProxyTest
{

//    private Collection<NodePropertyUpdate> lastUpdates;
//    @SuppressWarnings( "rawtypes" )
//    private final Answer<?> saveUpdatesInLastUpdatesField = new Answer()
//    {
//        @SuppressWarnings( "unchecked" )
//        @Override
//        public Object answer( InvocationOnMock invocationOnMock ) throws Throwable
//        {
//            lastUpdates = asCollection( (Iterable<NodePropertyUpdate>) invocationOnMock.getArguments()[0] );
//            return null;
//        }
//    };
//
//    @SuppressWarnings( "unchecked" )
//    @Test
//    public void shouldFilterUpdates() throws Exception
//    {
//        // Given
//        IndexProxy delegate = mock(IndexProxy.class);
//        when(delegate.getDescriptor()).thenReturn( new IndexDescriptor( 1337, 1337 ) );
//
//        doAnswer( saveUpdatesInLastUpdatesField ).when(delegate).update( anyCollection() );
//
//        RuleUpdateFilterIndexProxy indexProxy = new RuleUpdateFilterIndexProxy( delegate );
//
//        // When I send normal updates, things work
//        assertUpdates(       indexProxy, add( 1, 1337, 1, new long[]{1337} ) );
//
//        assertDoesntUpdate(  indexProxy, add( 1, 1336, 1, new long[]{1337} ) );
//        assertDoesntUpdate(  indexProxy, add( 1, 1337, 1, new long[]{1336} ) );
//    }
//
//    @SuppressWarnings( "unchecked" )
//    @Test
//    public void shouldFilterRecoveryUpdates() throws Exception
//    {
//        // Given
//        IndexProxy delegate = mock(IndexProxy.class);
//        when(delegate.getDescriptor()).thenReturn( new IndexDescriptor( 1337, 1337 ) );
//        doAnswer( saveUpdatesInLastUpdatesField ).when(delegate).recover( anyCollection() );
//
//        RuleUpdateFilterIndexProxy indexProxy = new RuleUpdateFilterIndexProxy( delegate );
//
//        // When I send recovery updates, things work
//        assertRecovers(      indexProxy, add( 1, 1337, 1, new long[]{1337} ) );
//
//        assertDoesntRecover( indexProxy, add( 1, 1336, 1, new long[]{1337} ) );
//        assertDoesntRecover( indexProxy, add( 1, 1337, 1, new long[]{1336} ) );
//    }
//
//    private void assertRecovers( IndexProxy outer, NodePropertyUpdate update ) throws IOException
//    {
//        outer.recover( asList( update ) );
//        assertEquals( asList( update ), lastUpdates );
//    }
//
//    private void assertDoesntRecover( IndexProxy outer, NodePropertyUpdate update ) throws IOException
//    {
//        outer.recover( asList( update ) );
//        assertEquals( 0, lastUpdates.size() );
//    }
//
//    private void assertUpdates( IndexProxy outer, NodePropertyUpdate update ) throws IOException
//    {
//        outer.update( asList( update) );
//        assertEquals( asList( update ), lastUpdates );
//    }
//
//    private void assertDoesntUpdate( IndexProxy outer, NodePropertyUpdate update ) throws IOException
//    {
//        outer.update( asList( update ) );
//        assertEquals( 0, lastUpdates.size() );
//    }
}
