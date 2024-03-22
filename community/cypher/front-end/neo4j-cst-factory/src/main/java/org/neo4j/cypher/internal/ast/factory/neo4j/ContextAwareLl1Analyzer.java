/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j;

import static org.antlr.v4.runtime.atn.LL1Analyzer.HIT_PRED;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNConfig;
import org.antlr.v4.runtime.atn.ATNState;
import org.antlr.v4.runtime.atn.AbstractPredicateTransition;
import org.antlr.v4.runtime.atn.EmptyPredictionContext;
import org.antlr.v4.runtime.atn.NotSetTransition;
import org.antlr.v4.runtime.atn.PredictionContext;
import org.antlr.v4.runtime.atn.RuleStopState;
import org.antlr.v4.runtime.atn.RuleTransition;
import org.antlr.v4.runtime.atn.SingletonPredictionContext;
import org.antlr.v4.runtime.atn.Transition;
import org.antlr.v4.runtime.atn.WildcardTransition;
import org.antlr.v4.runtime.misc.IntervalSet;

/**
 * Adapted from {@link org.antlr.v4.runtime.atn.LL1Analyzer}.
 * <pre>
 * ====================================================
 * Copyright (c) 2012-2017 The ANTLR Project. All rights reserved.
 * Use of this file is governed by the BSD 3-clause license that
 * can be found in the LICENSES.txt file in the project root.
 * ====================================================
 * </pre>
 */
public final class ContextAwareLl1Analyzer {
    private final ATN atn;

    public ContextAwareLl1Analyzer(ATN atn) {
        this.atn = atn;
    }

    public <T extends Look> T LOOK(ATNState s, RuleContext ctx, T look) {
        PredictionContext lookContext = ctx != null ? PredictionContext.fromRuleContext(s.atn, ctx) : null;
        _LOOK(s, null, lookContext, look, new HashSet<>(), new BitSet(), true, true);
        return look;
    }

    /*
     * This method is different from the one in org.antlr.v4.runtime.atn.LL1Analyzer in the following way.
     * Introduced Look to let you know the context of each suggestion.
     * Lines that do not include `look` are identical to the LL1Analyzer.
     */
    private void _LOOK(
            ATNState s,
            ATNState stopState,
            PredictionContext ctx,
            Look look,
            Set<ATNConfig> lookBusy,
            BitSet calledRuleStack,
            boolean seeThruPreds,
            boolean addEOF) {
        ATNConfig c = new ATNConfig(s, 0, ctx);
        if (!lookBusy.add(c)) return;

        if (s == stopState) {
            if (ctx == null) {
                look.expect(s.ruleIndex, calledRuleStack, Token.EPSILON);
                return;
            } else if (ctx.isEmpty() && addEOF) {
                look.expect(s.ruleIndex, calledRuleStack, Token.EOF);
                return;
            }
        }

        if (s instanceof RuleStopState) {
            if (ctx == null) {
                look.expect(s.ruleIndex, calledRuleStack, Token.EPSILON);
                return;
            } else if (ctx.isEmpty() && addEOF) {
                look.expect(s.ruleIndex, calledRuleStack, Token.EOF);
                return;
            }

            if (ctx != EmptyPredictionContext.Instance) {
                boolean removed = calledRuleStack.get(s.ruleIndex);
                try {
                    calledRuleStack.clear(s.ruleIndex);
                    for (int i = 0; i < ctx.size(); i++) {
                        ATNState returnState = atn.states.get(ctx.getReturnState(i));
                        _LOOK(
                                returnState,
                                stopState,
                                ctx.getParent(i),
                                look,
                                lookBusy,
                                calledRuleStack,
                                seeThruPreds,
                                addEOF);
                    }
                } finally {
                    if (removed) calledRuleStack.set(s.ruleIndex);
                }
                return;
            }
        }

        int n = s.getNumberOfTransitions();
        for (int i = 0; i < n; i++) {
            Transition t = s.transition(i);
            if (t.getClass() == RuleTransition.class) {
                if (calledRuleStack.get(t.target.ruleIndex)) {
                    continue;
                }

                var newContext = SingletonPredictionContext.create(ctx, ((RuleTransition) t).followState.stateNumber);

                try {
                    calledRuleStack.set(t.target.ruleIndex);
                    _LOOK(t.target, stopState, newContext, look, lookBusy, calledRuleStack, seeThruPreds, addEOF);
                } finally {
                    calledRuleStack.clear(t.target.ruleIndex);
                }
            } else if (t instanceof AbstractPredicateTransition) {
                if (seeThruPreds) {
                    _LOOK(t.target, stopState, ctx, look, lookBusy, calledRuleStack, seeThruPreds, addEOF);
                } else {
                    look.expect(s.ruleIndex, calledRuleStack, HIT_PRED);
                }
            } else if (t.isEpsilon()) {
                _LOOK(t.target, stopState, ctx, look, lookBusy, calledRuleStack, seeThruPreds, addEOF);
            } else if (t.getClass() == WildcardTransition.class) {
                look.expect(s.ruleIndex, calledRuleStack, IntervalSet.of(Token.MIN_USER_TOKEN_TYPE, atn.maxTokenType));
            } else {
                IntervalSet set = t.label();
                if (set != null) {
                    if (t instanceof NotSetTransition) {
                        set = set.complement(IntervalSet.of(Token.MIN_USER_TOKEN_TYPE, atn.maxTokenType));
                    }
                    look.expect(s.ruleIndex, calledRuleStack, set);
                }
            }
        }
    }

    public interface Look {
        /**
         * Add expected token.
         *
         * @param currentRule rule index which generated the expected token
         * @param rules rule call stack, only allowed to read during this call
         * @param tokenType token type that are expected at this point
         */
        void expect(int currentRule, BitSet rules, int tokenType);

        /**
         * Add expected tokens.
         *
         * @param currentRule rule index which generated the expected tokens
         * @param rules rule call stack, only allowed to read during this call
         * @param set set of token types that are expected at this point
         */
        void expect(int currentRule, BitSet rules, IntervalSet set);
    }
}
