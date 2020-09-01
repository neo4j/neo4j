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
package org.neo4j.kernel.impl.api;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class TokenHoldersIdLookupTest
{
    private static LoginContext.IdLookup idLookup;
    private static HashMap<String,Integer> name2id;

    @BeforeAll
    static void setup() throws KernelException
    {
        GlobalProcedures procs = new GlobalProceduresRegistry();
        procs.registerProcedure( TestProcedures.class );
        name2id = new HashMap<>();
        for ( ProcedureSignature signature : procs.getAllProcedures() )
        {
            QualifiedName name = signature.name();
            ProcedureHandle procedure = procs.procedure( name );
            name2id.put( name.toString(), procedure.id() );
        }
        idLookup = new TokenHoldersIdLookup( mockedTokenHolders(), procs );
    }

    private static TokenHolders mockedTokenHolders()
    {
        return new TokenHolders(
                mock( TokenHolder.class ),
                mock( TokenHolder.class ),
                mock( TokenHolder.class ) );
    }

    @Test
    void shouldLookupProcedureByName()
    {
        int[] ids = idLookup.getProcedureIds( "test.proc1" );
        assertThat( ids ).hasSize( 1 );
        assertThat( ids[0] ).isEqualTo( name2id.get( "test.proc1" ) );
    }

    @Test
    void shouldLookupAllProceduresWithStar()
    {
        int[] ids = idLookup.getProcedureIds( "*" );
        assertThat( ids ).hasSize( 6 );
        assertThat( ids ).containsExactlyInAnyOrder(
                name2id.get( "test.proc1" ),
                name2id.get( "test.proc2" ),
                name2id.get( "test.other.proc1" ),
                name2id.get( "test.other.proc42" ),
                name2id.get( "other.test.proc1" ),
                name2id.get( "test.(-_-).proc1" )
        );
    }

    @Test
    void shouldLookupEndingWith()
    {
        int[] ids = idLookup.getProcedureIds( "*.proc1" );
        assertThat( ids ).hasSize( 4 );
        assertThat( ids ).containsExactlyInAnyOrder(
                name2id.get( "test.proc1" ),
                name2id.get( "test.other.proc1" ),
                name2id.get( "other.test.proc1" ),
                name2id.get( "test.(-_-).proc1" )
        );
    }

    @Test
    void shouldLookupStartingWith()
    {
        int[] ids = idLookup.getProcedureIds( "other.*" );
        assertThat( ids ).hasSize( 1 );
        assertThat( ids[0] ).isEqualTo( name2id.get( "other.test.proc1" ) );
    }

    @Test
    void shouldLookupWithStarAndQuestionMark()
    {
        int[] ids = idLookup.getProcedureIds( "test.*.proc?" );
        assertThat( ids ).hasSize( 2 );
        assertThat( ids ).containsExactlyInAnyOrder(
                name2id.get( "test.other.proc1" ),
                name2id.get( "test.(-_-).proc1" )
        );

        ids = idLookup.getProcedureIds( "test.*.proc??" );
        assertThat( ids ).hasSize( 1 );
        assertThat( ids[0] ).isEqualTo( name2id.get( "test.other.proc42" ) );
    }

    @Test
    void shouldLookupWithMatchSingleCharacter()
    {
        int[] ids = idLookup.getProcedureIds( "test.proc?" );
        assertThat( ids ).hasSize( 2 );
        assertThat( ids ).containsExactlyInAnyOrder(
                name2id.get( "test.proc1" ),
                name2id.get( "test.proc2" )
        );
    }

    @Test
    void shouldMatchNoneWithMistake()
    {
        assertThat( idLookup.getProcedureIds( "test.?.proc1" ) ).isEmpty();
        assertThat( idLookup.getProcedureIds( "*.*.*.proc1" ) ).isEmpty();
        assertThat( idLookup.getProcedureIds( "*.proc" ) ).isEmpty();
    }

    @Test
    void matchingWithRegexFails()
    {
        // GRANT EXECUTE PROCEDURE `(\w+.)+\w+`
        assertThat( idLookup.getProcedureIds( "(\\w+.)+\\w+" ) ).isEmpty();
        // GRANT EXECUTE PROCEDURE `(\w+.)*\w+`
        assertThat( idLookup.getProcedureIds( "(\\w+.)*\\w+" ) ).isEmpty();
    }

    @Test
    void matchingWithSpecialCharacters()
    {
        // GRANT EXECUTE PROCEDURE `test.(-_-).proc1`
        int[] ids = idLookup.getProcedureIds( "test.(-_-).proc1" );
        assertThat( ids ).hasSize( 1 );
        assertThat( ids[0] ).isEqualTo( name2id.get( "test.(-_-).proc1" ) );
    }

    @SuppressWarnings( "unused" )
    public static class TestProcedures
    {
        @Procedure( name = "test.proc1" )
        public void proc1()
        {
        }

        @Procedure( name = "test.proc2" )
        public void proc2()
        {
        }

        @Procedure( name = "test.other.proc1" )
        public void proc3()
        {
        }

        @Procedure( name = "test.other.proc42" )
        public void proc4()
        {
        }

        @Procedure( name = "other.test.proc1" )
        public void proc5()
        {
        }

        @Procedure( name = "test.(-_-).proc1" )
        public void proc6()
        {
        }
    }
}
