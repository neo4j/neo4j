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
package org.neo4j.cypher.internal.literal.interpreter;

import java.time.Clock;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.neo4j.cypher.internal.parser.AstRuleCtx;
import org.neo4j.cypher.internal.parser.v5.Cypher5Lexer;
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser;
import org.neo4j.exceptions.SyntaxException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

public class Cypher5LiteralInterpreter {
    private Cypher5LiteralInterpreter() {}

    public static Object parseExpression(String cypherExpression) {
        final var builder = new LiteralInterpreterBuilder();
        var cypherLexer = new Cypher5Lexer(CharStreams.fromString(cypherExpression));
        var tokenStream = new CommonTokenStream(cypherLexer);
        var parser = new Cypher5Parser(tokenStream);
        parser.removeErrorListeners();
        var errorStrategy = new DefaultErrorStrategy();
        parser.setErrorHandler(errorStrategy);
        var errorListener = new LiteralErrorListener(cypherExpression);
        parser.addErrorListener(errorListener);
        parser.addParseListener(builder);
        final AstRuleCtx result;
        try {
            result = parser.expression();
        } catch (UnsupportedOperationException e) {
            if (errorListener.error != null) {
                throw errorListener.error;
            } else {
                throw e;
            }
        }

        if (errorListener.error != null) {
            throw errorListener.error;
        }
        if (!parser.isMatchedEOF() && parser.getInputStream().LA(1) != Token.EOF) {
            var offset = Optional.ofNullable(parser.getCurrentToken())
                    .map(Token::getStartIndex)
                    .orElse(0);
            throw new SyntaxException("Invalid cypher expression", cypherExpression, offset);
        }

        return result.ast;
    }
}

class LiteralInterpreterBuilder implements ParseTreeListener {
    public static final ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();
    private static final Object missingAst = new Object();

    @Override
    public void visitTerminal(TerminalNode terminalNode) {}

    @Override
    public void visitErrorNode(ErrorNode errorNode) {}

    @Override
    public void enterEveryRule(ParserRuleContext parserRuleContext) {}

    private void exitDefault(AstRuleCtx ctx) {
        if (ctx.getChildCount() == 1 && ctx.getChild(0) instanceof AstRuleCtx childCtx) {
            ctx.ast = childCtx.ast;
        }
    }

    private void throwUnsupportedQuery() {
        throw new UnsupportedOperationException("Query not supported in literal interpreter");
    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        if (ctx.exception != null) {
            return;
        }
        ((AstRuleCtx) ctx).ast = missingAst;
        switch (ctx.getRuleIndex()) {
            case Cypher5Parser.RULE_numberLiteral -> exitNumberLiteral((Cypher5Parser.NumberLiteralContext) ctx);
            case Cypher5Parser.RULE_literal -> exitLiteral((Cypher5Parser.LiteralContext) ctx);
            case Cypher5Parser.RULE_stringLiteral -> exitStringLiteral((Cypher5Parser.StringLiteralContext) ctx);
            case Cypher5Parser.RULE_listLiteral -> exitListLiteral((Cypher5Parser.ListLiteralContext) ctx);
            case Cypher5Parser.RULE_map -> exitMap((Cypher5Parser.MapContext) ctx);
            case Cypher5Parser.RULE_propertyKeyName -> exitPropertyKeyName((Cypher5Parser.PropertyKeyNameContext) ctx);
            case Cypher5Parser.RULE_symbolicNameString -> exitSymbolicNameString(
                    (Cypher5Parser.SymbolicNameStringContext) ctx);
            case Cypher5Parser.RULE_escapedSymbolicNameString -> exitEscapedSymbolicNameString(
                    (Cypher5Parser.EscapedSymbolicNameStringContext) ctx);
            case Cypher5Parser.RULE_unescapedSymbolicNameString -> exitUnescapedSymbolicNameString(
                    (Cypher5Parser.UnescapedSymbolicNameStringContext) ctx);
            case Cypher5Parser.RULE_functionInvocation -> exitFunctionInvocation(
                    (Cypher5Parser.FunctionInvocationContext) ctx);
            case Cypher5Parser.RULE_functionArgument -> exitFunctionArgument(
                    (Cypher5Parser.FunctionArgumentContext) ctx);
            case Cypher5Parser.RULE_functionName -> exitFunctionName((Cypher5Parser.FunctionNameContext) ctx);
            case Cypher5Parser.RULE_expression -> exitDefault((AstRuleCtx) ctx);
            case Cypher5Parser.RULE_expression1 -> exitExpression1((Cypher5Parser.Expression1Context) ctx);
            case Cypher5Parser.RULE_expression2 -> exitDefault((AstRuleCtx) ctx);
            case Cypher5Parser.RULE_expression3 -> exitDefault((AstRuleCtx) ctx);
            case Cypher5Parser.RULE_expression4 -> exitDefault((AstRuleCtx) ctx);
            case Cypher5Parser.RULE_expression5 -> exitDefault((AstRuleCtx) ctx);
            case Cypher5Parser.RULE_expression6 -> exitDefault((AstRuleCtx) ctx);
            case Cypher5Parser.RULE_expression7 -> exitDefault((AstRuleCtx) ctx);
            case Cypher5Parser.RULE_expression8 -> exitDefault((AstRuleCtx) ctx);
            case Cypher5Parser.RULE_expression9 -> exitDefault((AstRuleCtx) ctx);
            case Cypher5Parser.RULE_expression10 -> exitDefault((AstRuleCtx) ctx);
            case Cypher5Parser.RULE_expression11 -> exitDefault((AstRuleCtx) ctx);
            case Cypher5Parser.RULE_namespace -> exitNameSpace((Cypher5Parser.NamespaceContext) ctx);
            case Cypher5Parser.RULE_unescapedLabelSymbolicNameString -> exitUnescapedLabelSymbolicNameString(
                    (Cypher5Parser.UnescapedLabelSymbolicNameStringContext) ctx);

            default -> exitDefault((AstRuleCtx) ctx);
        }
        if (((AstRuleCtx) ctx).ast == missingAst) {
            throwUnsupportedQuery();
        }
    }

    private void exitFunctionArgument(Cypher5Parser.FunctionArgumentContext ctx) {
        if (ctx.getChildCount() == 1 && ctx.getChild(0) instanceof AstRuleCtx childCtx) {
            ctx.ast = childCtx.ast;
        }
    }

    private void exitFunctionInvocation(Cypher5Parser.FunctionInvocationContext ctx) {
        final ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();

        if (ctx.functionName().ast instanceof String functionName) {
            var arguments = ctx.functionArgument().stream().map(AstRuleCtx::ast).collect(Collectors.toList());
            switch (functionName) {
                case "date":
                    ctx.ast = createTemporalValue(
                            arguments, functionName, DateValue::now, DateValue::parse, DateValue::build);
                    break;
                case "datetime":
                    ctx.ast = createTemporalValue(
                            arguments,
                            functionName,
                            DateTimeValue::now,
                            s -> DateTimeValue.parse(s, () -> DEFAULT_ZONE_ID),
                            DateTimeValue::build);
                    break;
                case "time":
                    ctx.ast = createTemporalValue(
                            arguments,
                            functionName,
                            TimeValue::now,
                            s -> TimeValue.parse(s, () -> DEFAULT_ZONE_ID),
                            TimeValue::build);
                    break;
                case "localtime":
                    ctx.ast = createTemporalValue(
                            arguments, functionName, LocalTimeValue::now, LocalTimeValue::parse, LocalTimeValue::build);
                    break;
                case "localdatetime":
                    ctx.ast = createTemporalValue(
                            arguments,
                            functionName,
                            LocalDateTimeValue::now,
                            LocalDateTimeValue::parse,
                            LocalDateTimeValue::build);
                    break;
                case "duration":
                    ctx.ast = createDurationValue(arguments);
                    break;
                case "point":
                    ctx.ast = createPoint(arguments);
                    break;
                default:
                    throwUnsupportedQuery();
            }
        }
    }

    private static <T> T createTemporalValue(
            List<Object> arguments,
            String functionName,
            Function<Clock, T> onEmpty,
            Function<String, T> onString,
            BiFunction<MapValue, Supplier<ZoneId>, T> onMap) {
        if (arguments.isEmpty()) {
            return onEmpty.apply(Clock.system(DEFAULT_ZONE_ID));
        } else if (arguments.size() == 1) {
            Object date = arguments.get(0);
            if (date == null) {
                return null;
            } else if (date instanceof String) {
                return onString.apply((String) date);
            } else if (date instanceof Map) {
                @SuppressWarnings("unchecked")
                MapValue dateMap = asMapValue((Map<String, ?>) date);
                return onMap.apply(dateMap, () -> DEFAULT_ZONE_ID);
            }
        }

        throw new IllegalArgumentException("Function `" + functionName
                + "` did not get expected number of arguments: expected 0 or 1 argument, got " + arguments.size()
                + " arguments.");
    }

    private static MapValue asMapValue(Map<String, ?> map) {
        int size = map.size();
        if (size == 0) {
            return VirtualValues.EMPTY_MAP;
        }

        MapValueBuilder builder = new MapValueBuilder(size);
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            builder.add(entry.getKey(), Values.of(entry.getValue()));
        }
        return builder.build();
    }

    private static DurationValue createDurationValue(List<Object> arguments) {
        if (arguments.size() == 1) {
            Object duration = arguments.get(0);
            if (duration instanceof String) {
                return DurationValue.parse((String) duration);
            } else if (duration instanceof Map) {
                @SuppressWarnings("unchecked")
                MapValue dateMap = asMapValue((Map<String, ?>) duration);
                return DurationValue.build(dateMap);
            }
        }
        throw new IllegalArgumentException(
                "Function `duration` did not get expected number of arguments: expected 1 argument, got "
                        + arguments.size() + " arguments.");
    }

    private static PointValue createPoint(List<Object> arguments) {
        if (arguments.size() == 1) {
            Object point = arguments.get(0);
            if (point == null) {
                return null;
            } else if (point instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, ?> pointAsMap = (Map<String, ?>) point;
                return PointValue.fromMap(asMapValue(pointAsMap));
            } else {
                throw new IllegalArgumentException(
                        "Function `point` did not get expected argument. Expected a string or map input but got "
                                + point.getClass().getSimpleName() + ".");
            }
        } else {
            throw new IllegalArgumentException(
                    "Function `point` did not get expected number of arguments: expected 1 argument, got "
                            + arguments.size() + " arguments.");
        }
    }

    private void exitFunctionName(Cypher5Parser.FunctionNameContext ctx) {
        if (ctx.getChildCount() == 2 && ctx.getChild(1) instanceof AstRuleCtx childCtx) {
            ctx.ast = childCtx.ast;
        }
    }

    private void exitExpression1(Cypher5Parser.Expression1Context ctx) {
        if (ctx.literal() != null) {
            ctx.ast = ctx.literal().ast;
        } else if (ctx.listLiteral() != null) {
            ctx.ast = ctx.listLiteral().ast;
        } else if (ctx.functionInvocation() != null) {
            ctx.ast = ctx.functionInvocation().ast;
        }
    }

    private void exitListLiteral(Cypher5Parser.ListLiteralContext ctx) {
        ctx.ast = ctx.expression().stream().map(AstRuleCtx::ast).toList();
    }

    private void exitLiteral(Cypher5Parser.LiteralContext ctx) {
        if (ctx instanceof Cypher5Parser.NummericLiteralContext nctx) {
            ctx.ast = nctx.numberLiteral().ast;
        } else if (ctx instanceof Cypher5Parser.StringsLiteralContext sctx) {
            ctx.ast = sctx.stringLiteral().ast;
        } else if (ctx instanceof Cypher5Parser.OtherLiteralContext octx) {
            ctx.ast = octx.map().ast;
        } else if (ctx instanceof Cypher5Parser.BooleanLiteralContext bctx) {
            ctx.ast = bctx.TRUE() != null ? Boolean.TRUE : Boolean.FALSE;
        } else if (ctx instanceof Cypher5Parser.KeywordLiteralContext kctx) {
            if (kctx.INF() != null || kctx.INFINITY() != null) {
                ctx.ast = Double.POSITIVE_INFINITY;
            } else if (kctx.NAN() != null) {
                ctx.ast = Double.NaN;
            } else if (kctx.NULL() != null) {
                ctx.ast = null;
            }
        }
    }

    private void exitMap(Cypher5Parser.MapContext ctx) {
        var values = ctx.expression();
        var keys = ctx.propertyKeyName();
        int n = values.size();
        var map = new HashMap<String, Object>();
        for (int i = 0; i < n; i++) {
            map.put(keys.get(i).ast(), values.get(i).ast);
        }
        ctx.ast = map;
    }

    private void exitNumberLiteral(Cypher5Parser.NumberLiteralContext ctx) {
        if (ctx.DECIMAL_DOUBLE() != null) {
            ctx.ast = Double.parseDouble(ctx.getText());
        } else if (ctx.UNSIGNED_DECIMAL_INTEGER() != null) {
            var text = ctx.getText();
            ctx.ast = Long.parseLong(text);
        } else if (ctx.UNSIGNED_OCTAL_INTEGER() != null) {
            var octalString = ctx.getText().replaceFirst("o", "");
            ctx.ast = Long.parseLong(octalString, 8);
        } else if (ctx.UNSIGNED_HEX_INTEGER() != null) {
            var hexString = ctx.getText().replaceFirst("x", "");
            ctx.ast = Long.parseLong(hexString, 16);
        }
    }

    private void exitStringLiteral(Cypher5Parser.StringLiteralContext ctx) {
        ctx.ast = cypherStringToString(ctx.getText().substring(1, ctx.getText().length() - 1));
    }

    private void exitPropertyKeyName(Cypher5Parser.PropertyKeyNameContext ctx) {
        ctx.ast = ctx.symbolicNameString().ast;
    }

    private void exitSymbolicNameString(Cypher5Parser.SymbolicNameStringContext ctx) {
        ctx.ast = ctx.escapedSymbolicNameString() != null
                ? ctx.escapedSymbolicNameString().ast
                : ctx.unescapedSymbolicNameString().ast;
    }

    private void exitEscapedSymbolicNameString(Cypher5Parser.EscapedSymbolicNameStringContext ctx) {
        ctx.ast = ctx.getText().substring(1, ctx.getText().length() - 1);
    }

    private void exitUnescapedSymbolicNameString(Cypher5Parser.UnescapedSymbolicNameStringContext ctx) {
        ctx.ast = ctx.getText();
    }

    private void exitUnescapedLabelSymbolicNameString(Cypher5Parser.UnescapedLabelSymbolicNameStringContext ctx) {
        ctx.ast = ctx.getText();
    }

    private void exitNameSpace(Cypher5Parser.NamespaceContext ctx) {
        if (ctx.getChildCount() > 0) {
            throwUnsupportedQuery();
        }
        ctx.ast = null;
    }

    private String cypherStringToString(String input) {
        var pos = input.indexOf('\\');
        if (pos == -1) {
            return input;
        } else {
            var start = 0;
            var length = input.length();
            StringBuilder builder = null;
            while (pos != -1) {
                if (pos == length - 1)
                    throw new SyntaxException(
                            "Failed to parse string literal. The query must contain an even number of non-escaped quotes.");
                char replacement =
                        switch (input.charAt(pos + 1)) {
                            case 't' -> '\t';
                            case 'b' -> '\b';
                            case 'n' -> '\n';
                            case 'r' -> '\r';
                            case 'f' -> '\f';
                            case '\'' -> '\'';
                            case '"' -> '"';
                            case '\\' -> '\\';
                            case 'u' -> (char) Integer.parseInt(input.substring(pos + 2, pos + 2 + 4), 16);

                            default -> Character.MIN_VALUE;
                        };

                if (replacement != Character.MIN_VALUE) {
                    if (builder == null) builder = new java.lang.StringBuilder(input.length());
                    builder.append(input, start, pos).append(replacement);
                    start = input.charAt(pos + 1) == 'u' ? pos + 6 : pos + 2;
                }
                pos = input.indexOf('\\', pos + 2);
            }
            if (builder == null || builder.isEmpty()) return input;
            else if (start < input.length())
                return builder.append(input, start, input.length()).toString();
            else return builder.toString();
        }
    }
}

class LiteralErrorListener extends BaseErrorListener {

    SyntaxException error = null;
    private final String query;

    LiteralErrorListener(String query) {
        this.query = query;
    }

    @Override
    public void syntaxError(
            Recognizer<?, ?> recognizer,
            Object offendingSymbol,
            int line,
            int charPositionInLine,
            String msg,
            RecognitionException e) {
        var offset = offendingSymbol instanceof Token offendingToken ? offendingToken.getStartIndex() : 0;
        error = Exceptions.chain(error, new SyntaxException(msg, query, offset, e));
    }
}
