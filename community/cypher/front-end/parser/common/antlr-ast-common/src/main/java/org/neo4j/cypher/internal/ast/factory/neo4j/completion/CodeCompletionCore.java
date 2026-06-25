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
package org.neo4j.cypher.internal.ast.factory.neo4j.completion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.BufferedTokenStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNState;
import org.antlr.v4.runtime.atn.PredicateTransition;
import org.antlr.v4.runtime.atn.RuleStopState;
import org.antlr.v4.runtime.atn.RuleTransition;
import org.antlr.v4.runtime.atn.Transition;
import org.antlr.v4.runtime.misc.IntervalSet;
import org.neo4j.cypher.internal.parser.AstRuleCtx;
import org.neo4j.cypher.internal.parser.v25.Cypher25Parser;
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser;

/*
 * Adapted from https://github.com/mike-lischke/antlr4-c3/blob/c0530ed7e41911e734a5be75abf5d381589398b5/ports/java/src/main/java/com/vmware/antlr4c3/CodeCompletionCore.java#L41
 * Modifications:
 *   - Remove logging
 *
 * If you remove this file, also remove public/community/cypher/front-end/neo4j-cst-factory-license
 *
 * MIT License
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
 * See ThirdPartyLicenses.txt file for more info.
 */

/**
 * Port of antlr-c3 javascript library to java
 * <p>
 * The c3 engine is able to provide code completion candidates useful for
 * editors with ANTLR generated parsers, independent of the actual
 * language/grammar used for the generation.
 */
public class CodeCompletionCore {

    /**
     * JDO returning information about matching tokens and rules
     */
    public static class CandidatesCollection {
        /**
         * Collection of Token ID candidates, each with a follow-on List of
         * subsequent tokens
         */
        public Map<Integer, List<Integer>> tokens = new HashMap<>();
        /**
         * Collection of Rule candidates, each with the callstack of rules to
         * reach the candidate
         */
        public Map<Integer, CandidateRule> rules = new HashMap<>();

        @Override
        public String toString() {
            return "CandidatesCollection{" + "tokens=" + tokens + ", rules=" + rules + '}';
        }
    }

    public record RuleWithStartToken(int startTokenIndex, int ruleIndex) {}
    ;

    public record CandidateRule(int startTokenIndex, List<Integer> ruleList) {}
    ;

    public static class FollowSetWithPath {
        public IntervalSet intervals;
        public List<Integer> path;
        public List<Integer> following;
    }

    public static class FollowSetsHolder {
        public List<FollowSetWithPath> sets;
        public IntervalSet combined;
    }

    public static class PipelineEntry {

        public PipelineEntry(ATNState state, Integer tokenIndex) {
            this.state = state;
            this.tokenIndex = tokenIndex;
        }

        ATNState state;
        Integer tokenIndex;
    }

    private Set<Integer> ignoredTokens = new HashSet<>();
    private Set<Integer> preferredRules = new HashSet<>();

    private Parser parser;
    private ATN atn;
    private Vocabulary vocabulary;
    private String[] ruleNames;
    private List<Token> tokens;

    private int tokenStartIndex = 0;
    private int statesProcessed = 0;

    // A mapping of rule index to token stream position to end token positions.
    // A rule which has been visited before with the same input position will always produce the same output positions.
    private final RuleIndexCache<Map<Integer, Set<Integer>>> shortcutMap = new RuleIndexCache<>();

    private final CandidatesCollection candidates =
            new CandidatesCollection(); // The collected candidates (rules and tokens).

    private final Map<String, Map<Integer, FollowSetsHolder>> followSetsByATN = new HashMap<>();

    public CodeCompletionCore(Parser parser, Set<Integer> preferredRules, Set<Integer> ignoredTokens) {
        this.parser = parser;
        this.atn = parser.getATN();
        this.vocabulary = parser.getVocabulary();
        this.ruleNames = parser.getRuleNames();
        if (preferredRules != null) {
            this.preferredRules = preferredRules;
        }
        if (ignoredTokens != null) {
            this.ignoredTokens = ignoredTokens;
        }
    }

    public Set<Integer> getPreferredRules() {
        return Collections.unmodifiableSet(preferredRules);
    }

    public void setPreferredRules(Set<Integer> preferredRules) {
        this.preferredRules = new HashSet<>(preferredRules);
    }

    /**
     * This is the main entry point. The caret token index specifies the token stream index for the token which currently
     * covers the caret (or any other position you want to get code completion candidates for).
     * Optionally you can pass in a parser rule context which limits the ATN walk to only that or called rules. This can significantly
     * speed up the retrieval process but might miss some candidates (if they are outside of the given context).
     */
    public CandidatesCollection collectCandidates(int caretTokenIndex, ParserRuleContext context) {
        this.shortcutMap.clear();
        this.candidates.rules.clear();
        this.candidates.tokens.clear();
        this.statesProcessed = 0;

        if (context != null) {
            if (context instanceof AstRuleCtx ctx && ctx.lastClauseContext != null) {
                this.tokenStartIndex = ctx.lastClauseContext.start.getTokenIndex();
            } else {
                this.tokenStartIndex = context.start.getTokenIndex();
            }
        } else {
            this.tokenStartIndex = 0;
        }

        final var tokenStream = (BufferedTokenStream) this.parser.getInputStream();

        int currentIndex = tokenStream.index();
        tokenStream.seek(this.tokenStartIndex);
        this.tokens = new ArrayList<>();
        final var tokenSize = tokenStream.size();
        for (int offset = this.tokenStartIndex; offset < tokenSize; ++offset) {
            Token token = tokenStream.get(offset);
            if (token.getChannel() == Token.DEFAULT_CHANNEL) {
                this.tokens.add(token);
                if (token.getTokenIndex() >= caretTokenIndex || token.getType() == Token.EOF) {
                    break;
                }
            }
        }
        tokenStream.seek(currentIndex);

        LinkedList<RuleWithStartToken> callStack = new LinkedList<>();
        int startRule;
        if (context != null) {
            // If we have a lastClauseContext we want to use that as context for the start rule,
            // except if the current context is a StatementsContext, meaning the next token could be a new clause.
            if (context instanceof AstRuleCtx ctx
                    && !(ctx instanceof Cypher25Parser.StatementsContext)
                    && !(ctx instanceof Cypher5Parser.StatementsContext)
                    && ctx.lastClauseContext != null) {
                startRule = ctx.lastClauseContext.getRuleIndex();
            } else {
                startRule = context.getRuleIndex();
            }
        } else {
            startRule = 0;
        }

        this.processRule(this.atn.ruleToStartState[startRule], 0, callStack, "\n");

        tokenStream.seek(currentIndex);

        return this.candidates;
    }

    /**
     * Check if the predicate associated with the given transition evaluates to true.
     */
    private boolean checkPredicate(PredicateTransition transition) {
        return transition.getPredicate().eval(this.parser, ParserRuleContext.EMPTY);
    }

    /**
     * Walks the rule chain upwards to see if that matches any of the preferred rules.
     * If found, that rule is added to the collection candidates and true is returned.
     */
    private boolean translateToRuleIndex(List<RuleWithStartToken> ruleStack) {
        if (this.preferredRules.isEmpty()) return false;

        // Loop over the rule stack from highest to lowest rule level. This way we properly handle the higher rule
        // if it contains a lower one that is also a preferred rule.
        for (int i = 0; i < ruleStack.size(); ++i) {
            RuleWithStartToken current = ruleStack.get(i);
            int ruleIndex = current.ruleIndex;
            int startTokenIndex = current.startTokenIndex;
            if (this.preferredRules.contains(ruleIndex)) {
                // Add the rule to our candidates list along with the current rule path,
                // but only if there isn't already an entry like that.
                List<Integer> path = ruleStack.subList(0, i).stream()
                        .map(RuleWithStartToken::ruleIndex)
                        .collect(Collectors.toCollection(LinkedList::new));
                boolean addNew = true;
                for (Map.Entry<Integer, CandidateRule> entry : this.candidates.rules.entrySet()) {
                    if (!entry.getKey().equals(current.ruleIndex)
                            || entry.getValue().ruleList.size() != path.size()) {
                        continue;
                    }
                    // Found an entry for this rule. Same path? If so don't add a new (duplicate) entry.
                    if (path.equals(entry.getValue().ruleList)) {
                        addNew = false;
                        break;
                    }
                }

                if (addNew) {
                    this.candidates.rules.put(ruleIndex, new CandidateRule(startTokenIndex, path));
                }
                return true;
            }
        }

        return false;
    }

    /**
     * This method follows the given transition and collects all symbols within the same rule that directly follow it
     * without intermediate transitions to other rules and only if there is a single symbol for a transition.
     */
    private List<Integer> getFollowingTokens(Transition initialTransition) {
        LinkedList<Integer> result = new LinkedList<>();
        LinkedList<ATNState> seen = new LinkedList<>(); // unused but in orig
        LinkedList<ATNState> pipeline = new LinkedList<>();
        pipeline.add(initialTransition.target);

        while (!pipeline.isEmpty()) {
            ATNState state = pipeline.removeLast();

            for (Transition transition : state.getTransitions()) {
                if (transition.getSerializationType() == Transition.ATOM) {
                    if (!transition.isEpsilon()) {
                        List<Integer> list = transition.label().toList();
                        if (list.size() == 1 && !this.ignoredTokens.contains(list.get(0))) {
                            result.addLast(list.get(0));
                            pipeline.addLast(transition.target);
                        }
                    } else {
                        pipeline.addLast(transition.target);
                    }
                }
            }
        }

        return result;
    }

    /**
     * Entry point for the recursive follow set collection function.
     */
    private LinkedList<FollowSetWithPath> determineFollowSets(ATNState start, ATNState stop) {
        LinkedList<FollowSetWithPath> result = new LinkedList<>();
        Set<ATNState> seen = new HashSet<>();
        LinkedList<Integer> ruleStack = new LinkedList<>();

        this.collectFollowSets(start, stop, result, seen, ruleStack);

        return result;
    }

    /**
     * Collects possible tokens which could be matched following the given ATN state. This is essentially the same
     * algorithm as used in the LL1Analyzer class, but here we consider predicates also and use no parser rule context.
     */
    private void collectFollowSets(
            ATNState s,
            ATNState stopState,
            LinkedList<FollowSetWithPath> followSets,
            Set<ATNState> seen,
            LinkedList<Integer> ruleStack) {

        if (seen.contains(s)) return;

        seen.add(s);

        if (s.equals(stopState) || s.getStateType() == ATNState.RULE_STOP) {
            FollowSetWithPath set = new FollowSetWithPath();
            set.intervals = IntervalSet.of(Token.EPSILON);
            set.path = new LinkedList<Integer>(ruleStack);
            set.following = new LinkedList<Integer>();
            followSets.addLast(set);
            return;
        }

        for (Transition transition : s.getTransitions()) {
            if (transition.getSerializationType() == Transition.RULE) {
                RuleTransition ruleTransition = (RuleTransition) transition;
                if (ruleStack.indexOf(ruleTransition.target.ruleIndex) != -1) {
                    continue;
                }
                ruleStack.addLast(ruleTransition.target.ruleIndex);
                this.collectFollowSets(transition.target, stopState, followSets, seen, ruleStack);
                ruleStack.removeLast();

            } else if (transition.getSerializationType() == Transition.PREDICATE) {
                if (this.checkPredicate((PredicateTransition) transition)) {
                    this.collectFollowSets(transition.target, stopState, followSets, seen, ruleStack);
                }
            } else if (transition.isEpsilon()) {
                this.collectFollowSets(transition.target, stopState, followSets, seen, ruleStack);
            } else if (transition.getSerializationType() == Transition.WILDCARD) {
                FollowSetWithPath set = new FollowSetWithPath();
                set.intervals = IntervalSet.of(Token.MIN_USER_TOKEN_TYPE, this.atn.maxTokenType);
                set.path = new LinkedList<Integer>(ruleStack);
                set.following = new LinkedList<Integer>();
                followSets.addLast(set);
            } else {
                IntervalSet label = transition.label();
                if (label != null && label.size() > 0) {
                    if (transition.getSerializationType() == Transition.NOT_SET) {
                        label = label.complement(IntervalSet.of(Token.MIN_USER_TOKEN_TYPE, this.atn.maxTokenType));
                    }
                    FollowSetWithPath set = new FollowSetWithPath();
                    set.intervals = label;
                    set.path = new LinkedList<Integer>(ruleStack);
                    set.following = this.getFollowingTokens(transition);
                    followSets.addLast(set);
                }
            }
        }
    }

    /**
     * Walks the ATN for a single rule only. It returns the token stream position for each path that could be matched in this rule.
     * The result can be empty in case we hit only non-epsilon transitions that didn't match the current input or if we
     * hit the caret position.
     */
    private Set<Integer> processRule(
            ATNState startState, int tokenIndex, LinkedList<RuleWithStartToken> callStack, String indentation) {
        // Start with rule specific handling before going into the ATN walk.
        // Check first if we've taken this path with the same input before.
        Map<Integer, Set<Integer>> positionMap = this.shortcutMap.get(startState.ruleIndex);
        if (positionMap == null) {
            positionMap = new HashMap<>();
            this.shortcutMap.put(startState.ruleIndex, positionMap);
        } else {
            if (positionMap.containsKey(tokenIndex)) {
                return positionMap.get(tokenIndex);
            }
        }

        Set<Integer> result = new HashSet<>();

        // For rule start states we determine and cache the follow set, which gives us 3 advantages:
        // 1) We can quickly check if a symbol would be matched when we follow that rule. We can so check in advance
        //    and can save us all the intermediate steps if there is no match.
        // 2) We'll have all symbols that are collectable already together when we are at the caret when entering a
        // rule.
        // 3) We get this lookup for free with any 2nd or further visit of the same rule, which often happens
        //    in non trivial grammars, especially with (recursive) expressions and of course when invoking code
        // completion
        //    multiple times.
        Map<Integer, FollowSetsHolder> setsPerState =
                followSetsByATN.get(this.parser.getClass().getName());
        if (setsPerState == null) {
            setsPerState = new HashMap<>();
            followSetsByATN.put(this.parser.getClass().getName(), setsPerState);
        }

        FollowSetsHolder followSets = setsPerState.get(startState.stateNumber);
        if (followSets == null) {
            followSets = new FollowSetsHolder();
            setsPerState.put(startState.stateNumber, followSets);
            RuleStopState stop = this.atn.ruleToStopState[startState.ruleIndex];
            followSets.sets = this.determineFollowSets(startState, stop);

            // Sets are split by path to allow translating them to preferred rules. But for quick hit tests
            // it is also useful to have a set with all symbols combined.
            IntervalSet combined = new IntervalSet();
            for (FollowSetWithPath set : followSets.sets) {
                combined.addAll(set.intervals);
            }
            followSets.combined = combined;
        }

        int startTokenIndex = this.tokens.get(tokenIndex).getTokenIndex();
        callStack.addLast(new RuleWithStartToken(startTokenIndex, startState.ruleIndex));
        int currentSymbol = this.tokens.get(tokenIndex).getType();

        if (tokenIndex >= this.tokens.size() - 1) { // At caret?

            if (this.preferredRules.contains(startState.ruleIndex)) {
                // No need to go deeper when collecting entries and we reach a rule that we want to collect anyway.
                this.translateToRuleIndex(callStack);
            } else {
                // Convert all follow sets to either single symbols or their associated preferred rule and add
                // the result to our candidates list.
                for (FollowSetWithPath set : followSets.sets) {
                    LinkedList<RuleWithStartToken> fullPath = new LinkedList<>(callStack);
                    List<RuleWithStartToken> followSetPath = set.path.stream()
                            .map((path) -> new RuleWithStartToken(startTokenIndex, path))
                            .toList();
                    fullPath.addAll(followSetPath);
                    if (!this.translateToRuleIndex(fullPath)) {
                        for (int symbol : set.intervals.toList()) {
                            if (!this.ignoredTokens.contains(symbol)) {
                                if (!this.candidates.tokens.containsKey(symbol))
                                    this.candidates.tokens.put(
                                            symbol,
                                            set.following); // Following is empty if there is more than one entry in the
                                // set.
                                else {
                                    // More than one following list for the same symbol.
                                    if (!this.candidates.tokens.get(symbol).equals(set.following)) { // XXX js uses !=
                                        this.candidates.tokens.put(symbol, new LinkedList<Integer>());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            callStack.removeLast();
            return result;

        } else {
            // Process the rule if we either could pass it without consuming anything (epsilon transition)
            // or if the current input symbol will be matched somewhere after this entry point.
            // Otherwise stop here.
            if (!followSets.combined.contains(Token.EPSILON) && !followSets.combined.contains(currentSymbol)) {
                callStack.removeLast();
                return result;
            }
        }

        // The current state execution pipeline contains all yet-to-be-processed ATN states in this rule.
        // For each such state we store the token index + a list of rules that lead to it.
        LinkedList<PipelineEntry> statePipeline = new LinkedList<>();
        PipelineEntry currentEntry;

        // Bootstrap the pipeline.
        statePipeline.add(new PipelineEntry(startState, tokenIndex));

        while (!statePipeline.isEmpty()) {
            currentEntry = statePipeline.removeLast();
            ++this.statesProcessed;

            currentSymbol = this.tokens.get(currentEntry.tokenIndex).getType();

            boolean atCaret = currentEntry.tokenIndex >= this.tokens.size() - 1;

            switch (currentEntry.state.getStateType()) {
                case ATNState.RULE_START: // Happens only for the first state in this rule, not subrules.
                    indentation += "  ";
                    break;

                case ATNState.RULE_STOP: {
                    // Record the token index we are at, to report it to the caller.
                    result.add(currentEntry.tokenIndex);
                    continue;
                }

                default:
                    break;
            }

            Transition[] transitions = currentEntry.state.getTransitions();
            for (Transition transition : transitions) {
                switch (transition.getSerializationType()) {
                    case Transition.RULE: {
                        Set<Integer> endStatus =
                                this.processRule(transition.target, currentEntry.tokenIndex, callStack, indentation);
                        for (Integer position : endStatus) {
                            statePipeline.addLast(
                                    new PipelineEntry(((RuleTransition) transition).followState, position));
                        }
                        break;
                    }

                    case Transition.PREDICATE: {
                        if (this.checkPredicate((PredicateTransition) transition)) {
                            statePipeline.addLast(new PipelineEntry(transition.target, currentEntry.tokenIndex));
                        }
                        break;
                    }

                    case Transition.WILDCARD: {
                        if (atCaret) {
                            if (!this.translateToRuleIndex(callStack)) {
                                for (Integer token : IntervalSet.of(Token.MIN_USER_TOKEN_TYPE, this.atn.maxTokenType)
                                        .toList()) {
                                    if (!this.ignoredTokens.contains(token)) {
                                        this.candidates.tokens.put(token, new LinkedList<Integer>());
                                    }
                                }
                            }
                        } else {
                            statePipeline.addLast(new PipelineEntry(transition.target, currentEntry.tokenIndex + 1));
                        }
                        break;
                    }

                    default: {
                        if (transition.isEpsilon()) {
                            // Jump over simple states with a single outgoing epsilon transition.
                            statePipeline.addLast(new PipelineEntry(transition.target, currentEntry.tokenIndex));
                            continue;
                        }

                        IntervalSet set = transition.label();
                        if (set != null && set.size() > 0) {
                            if (transition.getSerializationType() == Transition.NOT_SET) {
                                set = set.complement(IntervalSet.of(Token.MIN_USER_TOKEN_TYPE, this.atn.maxTokenType));
                            }
                            if (atCaret) {
                                if (!this.translateToRuleIndex(callStack)) {
                                    List<Integer> list = set.toList();
                                    boolean addFollowing = list.size() == 1;
                                    for (Integer symbol : list) {
                                        if (!this.ignoredTokens.contains(symbol)) {
                                            if (addFollowing) {
                                                this.candidates.tokens.put(symbol, this.getFollowingTokens(transition));
                                            } else {
                                                this.candidates.tokens.put(symbol, new LinkedList<>());
                                            }
                                        }
                                    }
                                }
                            } else {
                                if (set.contains(currentSymbol)) {
                                    statePipeline.addLast(
                                            new PipelineEntry(transition.target, currentEntry.tokenIndex + 1));
                                }
                            }
                        }
                    }
                }
            }
        }

        callStack.removeLast();

        // Cache the result, for later lookup to avoid duplicate walks.
        positionMap.put(tokenIndex, result);

        return result;
    }
}

class RuleIndexCache<T> {
    // This list will at most have the same size as the number of rules in the grammar (currently 369 in Cypher 25).
    private final ArrayList<T> map = new ArrayList<>();

    public void clear() {
        map.clear();
    }

    public T get(int ruleIndex) {
        return ruleIndex >= 0 && ruleIndex < map.size() ? map.get(ruleIndex) : null;
    }

    public void put(int ruleIndex, T positionMap) {
        if (ruleIndex < 0) return; // Rule index can't be negative, but let's be safe
        map.ensureCapacity(ruleIndex + 1);
        while (map.size() <= ruleIndex) map.add(null);
        map.set(ruleIndex, positionMap);
    }
}
