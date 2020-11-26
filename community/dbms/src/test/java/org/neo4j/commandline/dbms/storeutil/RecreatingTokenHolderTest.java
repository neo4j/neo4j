/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.commandline.dbms.storeutil;

import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.logging.NullLog;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( RandomExtension.class )
class RecreatingTokenHolderTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldRecreateMissingTokensConcurrently()
    {
        // given
        MutableMap<String,List<NamedToken>> recreatedTokens = Maps.mutable.empty();
        String type = TokenHolder.TYPE_PROPERTY_KEY;
        RecreatingTokenHolder tokenHolder = new RecreatingTokenHolder( type, new StoreCopyStats( NullLog.getInstance() ), recreatedTokens );
        List<NamedToken> initialTokens = new ArrayList<>();
        MutableIntSet missing = IntSets.mutable.empty();
        int highTokenId = buildInitialTokens( tokenHolder, initialTokens, missing );

        // when
        Race race = new Race().withEndCondition( () ->
        {
            List<NamedToken> recreated = recreatedTokens.get( type );
            int numRecreated = recreated != null ? recreated.size() : 0;
            return numRecreated == missing.size();
        } );
        race.addContestants( Runtime.getRuntime().availableProcessors(), () -> tokenHolder.getTokenById( random.nextInt( highTokenId ) ) );
        race.goUnchecked();

        // then
        for ( NamedToken recreatedToken : recreatedTokens.get( type ) )
        {
            assertTrue( missing.remove( recreatedToken.id() ) );
        }
        assertTrue( missing.isEmpty() );
        Set<String> names = new HashSet<>();
        MutableIntSet ids = IntSets.mutable.empty();
        for ( NamedToken token : tokenHolder.getAllTokens() )
        {
            assertTrue( names.add( token.name() ) );
            assertTrue( ids.add( token.id() ) );
        }
    }

    private int buildInitialTokens( RecreatingTokenHolder tokenHolder, List<NamedToken> initialTokens, MutableIntSet missing )
    {
        int numExistingTokens = random.nextInt( 5, 10 );
        int highId = 0;
        for ( int i = 0; i < numExistingTokens; i++ )
        {
            int id = highId++;
            initialTokens.add( new NamedToken( "initial-" + id, id ) );
        }
        int numMissing = random.nextInt( 1, numExistingTokens - 1 );
        for ( int i = 0; i < numMissing; i++ )
        {
            NamedToken removed = random.among( initialTokens );
            initialTokens.remove( removed );
            missing.add( removed.id() );
        }

        // add some tokens that would collide with the recreated token names
        MutableInt recreatedNumber = new MutableInt();
        int numCollisions = random.nextInt( 1, missing.size() );
        for ( int i = 0; i < numCollisions; i++ )
        {
            initialTokens.add( new NamedToken( tokenHolder.generateRecreatedTokenName( recreatedNumber.incrementAndGet() ), highId++ ) );
        }
        tokenHolder.setInitialTokens( initialTokens );
        return highId;
    }
}
