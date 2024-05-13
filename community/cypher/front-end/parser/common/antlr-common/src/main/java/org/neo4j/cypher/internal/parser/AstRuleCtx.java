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
package org.neo4j.cypher.internal.parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

public class AstRuleCtx extends ParserRuleContext {
    public Object ast;

    public AstRuleCtx() {}

    public AstRuleCtx(ParserRuleContext parent, int invokingStateNumber) {
        super(parent, invokingStateNumber);

        /*
         * The default size of array lists are 10.
         * But it's very common that we have fewer children.
         * When parsing all the macro benchmark queries,
         * 0.5% of contexts had 0 children,
         * 90% had a single child or less,
         * 95% had two or less,
         * 97% had three or less.
         */
        this.children = new ArrayList<>(1);
    }

    @SuppressWarnings("unchecked")
    public final <T> T ast() {
        return (T) ast;
    }

    @Override
    public final String getText() {
        final var size = children.size();
        return switch (size) {
            case 0 -> "";
            case 1 -> children.get(0).getText();
            default -> {
                final var builder = new StringBuilder();
                for (int i = 0; i < size; i++) builder.append(children.get(i).getText());
                yield builder.toString();
            }
        };
    }

    @Override
    public final TerminalNode getToken(int targetType, int targetIndex) {
        int tokenIndex = -1;
        final int size = children.size();
        for (int i = 0; i < size; ++i) {
            if (children.get(i) instanceof TerminalNode node
                    && node.getSymbol().getType() == targetType
                    && ++tokenIndex == targetIndex) {
                return node;
            }
        }

        return null;
    }

    @Override
    public final List<TerminalNode> getTokens(int targetType) {
        List<TerminalNode> tokens = null;
        final int size = children.size();
        for (int i = 0; i < size; ++i) {
            if (children.get(i) instanceof TerminalNode node && node.getSymbol().getType() == targetType) {
                if (tokens == null) tokens = new ArrayList<>();
                tokens.add(node);
            }
        }

        return tokens != null ? tokens : Collections.emptyList();
    }

    @Override
    public final <T extends ParserRuleContext> List<T> getRuleContexts(Class<? extends T> ctxType) {
        List<T> contexts = null;
        final int size = children.size();
        for (int i = 0; i < size; ++i) {
            final var o = children.get(i);
            if (ctxType.isInstance(o)) {
                if (contexts == null) contexts = new ArrayList<>();
                contexts.add(ctxType.cast(o));
            }
        }

        return contexts != null ? contexts : Collections.emptyList();
    }

    @Override
    public final <T extends ParseTree> T getChild(Class<? extends T> ctxType, int targetIndex) {
        int childIndex = -1;
        final int size = children.size();
        for (int i = 0; i < size; ++i) {
            final var o = children.get(i);
            if (ctxType.isInstance(o) && ++childIndex == targetIndex) {
                return ctxType.cast(o);
            }
        }
        return null;
    }

    @Override
    public final ParseTree getChild(int i) {
        return children.get(i);
    }
}
