/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.procedure.UserFunction;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

class TokenHoldersIdLookupTest {
    private static LoginContext.IdLookup idLookup;
    private static HashMap<String, Integer> procName2id;
    private static HashMap<String, Integer> funcName2id;

    @BeforeAll
    static void setup() throws KernelException {
        GlobalProcedures reg = new GlobalProceduresRegistry();
        reg.registerProcedure(TestProcedures.class);
        reg.registerFunction(TestProcedures.class);
        reg.registerAggregationFunction(TestProcedures.class);
        var procs = reg.getCurrentView();
        procName2id = new HashMap<>();
        for (ProcedureSignature signature :
                procs.getAllProcedures(CypherScope.CYPHER_5).toList()) {
            QualifiedName name = signature.name();
            ProcedureHandle procedure = procs.procedure(name, CypherScope.CYPHER_5);
            procName2id.put(name.toString(), procedure.id());
        }
        funcName2id = new HashMap<>();
        procs.getAllNonAggregatingFunctions(CypherScope.CYPHER_5).forEach(signature -> {
            QualifiedName name = signature.name();
            UserFunctionHandle function = procs.function(name, CypherScope.CYPHER_5);
            funcName2id.put(name.toString(), function.id());
        });
        procs.getAllAggregatingFunctions(CypherScope.CYPHER_5).forEach(signature -> {
            QualifiedName name = signature.name();
            UserFunctionHandle function = procs.aggregationFunction(name, CypherScope.CYPHER_5);
            funcName2id.put(name.toString(), function.id());
        });
        idLookup = new TokenHoldersIdLookup(mockedTokenHolders(), procs, () -> false);
    }

    private static TokenHolders mockedTokenHolders() {
        return new TokenHolders(mock(TokenHolder.class), mock(TokenHolder.class), mock(TokenHolder.class));
    }

    @Test
    void shouldLookupProcedureByName() {
        int[] ids = idLookup.getProcedureIds("test.proc1");
        assertThat(ids).hasSize(1);
        assertThat(ids[0]).isEqualTo(procName2id.get("test.proc1"));
    }

    @Test
    void shouldLookupAllProceduresWithStar() {
        int[] ids = idLookup.getProcedureIds("*");
        assertThat(ids).hasSize(6);
        assertThat(ids)
                .containsExactlyInAnyOrder(
                        procName2id.get("test.proc1"),
                        procName2id.get("test.proc2"),
                        procName2id.get("test.other.proc1"),
                        procName2id.get("test.other.proc42"),
                        procName2id.get("other.test.proc1"),
                        procName2id.get("test.(-_-).proc1"));
    }

    @Test
    void shouldLookupProcedureEndingWith() {
        int[] ids = idLookup.getProcedureIds("*.proc1");
        assertThat(ids).hasSize(4);
        assertThat(ids)
                .containsExactlyInAnyOrder(
                        procName2id.get("test.proc1"),
                        procName2id.get("test.other.proc1"),
                        procName2id.get("other.test.proc1"),
                        procName2id.get("test.(-_-).proc1"));
    }

    @Test
    void shouldLookupProcedureStartingWith() {
        int[] ids = idLookup.getProcedureIds("other.*");
        assertThat(ids).hasSize(1);
        assertThat(ids[0]).isEqualTo(procName2id.get("other.test.proc1"));
    }

    @Test
    void shouldLookupWithStarAndQuestionMark() {
        int[] ids = idLookup.getProcedureIds("test.*.proc?");
        assertThat(ids).hasSize(2);
        assertThat(ids)
                .containsExactlyInAnyOrder(procName2id.get("test.other.proc1"), procName2id.get("test.(-_-).proc1"));

        ids = idLookup.getProcedureIds("test.*.proc??");
        assertThat(ids).hasSize(1);
        assertThat(ids[0]).isEqualTo(procName2id.get("test.other.proc42"));
    }

    @Test
    void shouldLookupWithMatchSingleCharacter() {
        int[] ids = idLookup.getProcedureIds("test.proc?");
        assertThat(ids).hasSize(2);
        assertThat(ids).containsExactlyInAnyOrder(procName2id.get("test.proc1"), procName2id.get("test.proc2"));
    }

    @Test
    void shouldMatchNoneWithMistake() {
        assertThat(idLookup.getProcedureIds("test.?.proc1")).isEmpty();
        assertThat(idLookup.getProcedureIds("*.*.*.proc1")).isEmpty();
        assertThat(idLookup.getProcedureIds("*.proc")).isEmpty();
    }

    @Test
    void matchingWithRegexFails() {
        // GRANT EXECUTE PROCEDURE `(\w+.)+\w+`
        assertThat(idLookup.getProcedureIds("(\\w+.)+\\w+")).isEmpty();
        // GRANT EXECUTE PROCEDURE `(\w+.)*\w+`
        assertThat(idLookup.getProcedureIds("(\\w+.)*\\w+")).isEmpty();
    }

    @Test
    void matchingWithSpecialCharacters() {
        // GRANT EXECUTE PROCEDURE `test.(-_-).proc1`
        int[] ids = idLookup.getProcedureIds("test.(-_-).proc1");
        assertThat(ids).hasSize(1);
        assertThat(ids[0]).isEqualTo(procName2id.get("test.(-_-).proc1"));
    }

    @Test
    void shouldLookupFunctionByName() {
        int[] ids = idLookup.getFunctionIds("test.func1");
        assertThat(ids).hasSize(1);
        assertThat(ids[0]).isEqualTo(funcName2id.get("test.func1"));
    }

    @Test
    void shouldLookupAllFunctionsWithStar() {
        int[] ids = idLookup.getFunctionIds("*");
        assertThat(ids).hasSize(2);
        assertThat(ids).containsExactlyInAnyOrder(funcName2id.get("test.func1"), funcName2id.get("test.func42"));
    }

    @Test
    void shouldLookupFunctionEndingWith() {
        int[] ids = idLookup.getFunctionIds("*.func1");
        assertThat(ids).hasSize(1);
        assertThat(ids[0]).isEqualTo(funcName2id.get("test.func1"));
    }

    @Test
    void shouldLookupFunctionStartingWith() {
        int[] ids = idLookup.getFunctionIds("test.*");
        assertThat(ids).hasSize(2);
        assertThat(ids).containsExactlyInAnyOrder(funcName2id.get("test.func1"), funcName2id.get("test.func42"));
    }

    @Test
    void shouldLookupFunctionWithStarAndQuestionMark() {
        int[] ids = idLookup.getFunctionIds("te*.func?");
        assertThat(ids).hasSize(1);
        assertThat(ids[0]).isEqualTo(funcName2id.get("test.func1"));

        ids = idLookup.getFunctionIds("tes*.func??");
        assertThat(ids).hasSize(1);
        assertThat(ids[0]).isEqualTo(funcName2id.get("test.func42"));

        ids = idLookup.getFunctionIds("tes*.fun?42");
        assertThat(ids).hasSize(1);
        assertThat(ids[0]).isEqualTo(funcName2id.get("test.func42"));
    }

    public static class TestProcedures {
        @Procedure(name = "test.proc1")
        public void proc1() {}

        @Procedure(name = "test.proc2")
        public void proc2() {}

        @Procedure(name = "test.other.proc1")
        public void proc3() {}

        @Procedure(name = "test.other.proc42")
        public void proc4() {}

        @Procedure(name = "other.test.proc1")
        public void proc5() {}

        @Procedure(name = "test.(-_-).proc1")
        public void proc6() {}

        @UserFunction("test.func1")
        public long func1() {
            return 0;
        }

        @UserFunction("test.func42")
        public long func2() {
            return 0;
        }

        @UserAggregationFunction("test.agg.func1")
        public AggFunc1 aggFunc1() {
            return new AggFunc1();
        }

        @UserAggregationFunction("test.agg.(-_-).func1")
        public AggFunc2 aggFunc2() {
            return new AggFunc2();
        }

        public static class AggFunc1 {
            @UserAggregationUpdate()
            public void update(@Name("in") long in) {}

            @UserAggregationResult
            public long result() {
                return 42;
            }
        }

        public static class AggFunc2 {
            @UserAggregationUpdate()
            public void update(@Name("in") long in) {}

            @UserAggregationResult
            public long result() {
                return 42;
            }
        }
    }
}
