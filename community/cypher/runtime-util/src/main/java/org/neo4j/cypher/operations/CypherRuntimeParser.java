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
package org.neo4j.cypher.operations;

import static org.neo4j.cypher.operations.VectorUtils.safeCastToByte;
import static org.neo4j.cypher.operations.VectorUtils.safeCastToInt;
import static org.neo4j.cypher.operations.VectorUtils.safeCastToLong;
import static org.neo4j.cypher.operations.VectorUtils.safeCastToShort;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static scala.jdk.CollectionConverters.CollectionHasAsScala;

import java.math.BigDecimal;
import java.util.List;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature;
import org.neo4j.cypher.internal.expressions.Expression;
import org.neo4j.cypher.internal.expressions.ListLiteral;
import org.neo4j.cypher.internal.expressions.Literal;
import org.neo4j.cypher.internal.expressions.NumberLiteral;
import org.neo4j.cypher.internal.parser.AstParserFactory;
import org.neo4j.cypher.internal.parser.AstParserFactory$;
import org.neo4j.cypher.internal.parser.ast.AstParser;
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.exceptions.SyntaxException;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;
import scala.Option;
import scala.collection.immutable.Seq;

/**
 * Utility class for parsing strings from the cypher runtime.
 * <p>
 * Internally uses the cypher parser so that we can make sure we are consistent with the language itself
 */
abstract class CypherRuntimeParser {
    private static final BigDecimal MAX_LONG = BigDecimal.valueOf(Long.MAX_VALUE);
    private static final BigDecimal MIN_LONG = BigDecimal.valueOf(Long.MIN_VALUE);

    private CypherRuntimeParser() {
        throw new UnsupportedOperationException();
    }

    // NOTE: we are using the Cypher25 parser only, so far we only parse very simple things like numbers and lists
    //      of numbers so the version shouldn't matter. If we ever do more advanced stuff that will be depending on the
    //      version we will probably need to make this a dynamic class and provide the version as a dependency.
    private static final AstParserFactory parserFactory = AstParserFactory$.MODULE$.apply(CypherVersion.Cypher25);

    static VectorValue parseInt8Vector(String expression) {
        var expressions = asList(expression);
        int length = expressions.size();
        byte[] bytes = new byte[length];
        var iterator = expressions.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            bytes[i++] = safeCastToByte(asLong(iterator.next()));
        }
        return Values.int8Vector(bytes);
    }

    static VectorValue parseInt16Vector(String expression) {
        var expressions = asList(expression);
        int length = expressions.size();
        short[] shorts = new short[length];
        var iterator = expressions.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            shorts[i++] = safeCastToShort(asLong(iterator.next()));
        }
        return Values.int16Vector(shorts);
    }

    static VectorValue parseInt32Vector(String expression) {
        var expressions = asList(expression);
        int length = expressions.size();
        int[] ints = new int[length];
        var iterator = expressions.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            ints[i++] = safeCastToInt(asLong(iterator.next()));
        }
        return Values.int32Vector(ints);
    }

    static VectorValue parseInt64Vector(String expression) {
        var expressions = asList(expression);
        int length = expressions.size();
        long[] longs = new long[length];
        var iterator = expressions.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            longs[i++] = asLong(iterator.next());
        }
        return Values.int64Vector(longs);
    }

    static VectorValue parseFloat32Vector(String expression) {
        var expressions = asList(expression);
        int length = expressions.size();
        float[] floats = new float[length];
        var iterator = expressions.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            floats[i++] = asNumber(iterator.next()).floatValue();
        }
        return Values.float32Vector(floats);
    }

    static VectorValue parseFloat64Vector(String expression) {
        var expressions = asList(expression);
        int length = expressions.size();
        double[] doubles = new double[length];
        var iterator = expressions.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            doubles[i++] = asNumber(iterator.next()).doubleValue();
        }
        return Values.float64Vector(doubles);
    }

    /**
     * Tries to parse the specified expression as a Cypher number literal.
     * Returns the parsed value converted to DoubleValue, or NoValue if parsing fails.
     */
    public static Value parseAsDoubleOrElseNoValue(String expression) {
        try {
            // This route is significantly faster than parsing cypher, so try this first.
            return doubleValue(Double.parseDouble(expression));
        } catch (NumberFormatException ignore1) {
            try {
                // Fallback to parsing the expression in Cypher.
                return doubleValue(parser(expression).numberLiteral().value().doubleValue());
            } catch (NumberFormatException | SyntaxException ignore2) {
                return NO_VALUE;
            }
        }
    }

    /**
     * Tries to parse the specified expression as a Cypher number literal.
     * Returns the parsed value converted to LongValue, or NoValue if parsing fails.
     */
    public static Value parseAsLongOrElseNoValue(String expression) {
        String expressionTrimmed = expression.trim();
        // This route is significantly faster than parsing cypher, so try this first.
        // Note, parseLong also supports some values that are not valid cypher, like 0001.
        var res = parseLongWithoutThrowing(expressionTrimmed);
        if (res != NO_VALUE) {
            return res;
        }

        try {
            // Note, BigDecimal supports expressions with exponent, like -1.23E-12.
            BigDecimal bigDecimal = new BigDecimal(expressionTrimmed);
            if (bigDecimal.compareTo(MAX_LONG) <= 0 && bigDecimal.compareTo(MIN_LONG) >= 0) {
                return longValue(bigDecimal.longValue());
            } else {
                // This can happen for the functions toInteger(input), toIntegerOrNull(input) and
                // toIntegerList(input),
                // but will only surface for toInteger() as the others convert errors to null values
                throw CypherTypeException.integerOutOfBounds(
                        "input", Long.MIN_VALUE, Long.MAX_VALUE, expressionTrimmed);
            }
        } catch (NumberFormatException ignore2) {
            // Fallback to parsing the expression in Cypher.
            // Note, adds support to more literal number expressions, like 0xf (hex), 0o11 (octal), 1_000.
            try {
                return longValue(
                        parser(expressionTrimmed).numberLiteral().value().longValue());
            } catch (NumberFormatException | SyntaxException ignore3) {
                return NO_VALUE;
            }
        }
    }

    // copied from Long.parseLong but does not throw exception
    private static Value parseLongWithoutThrowing(String s) {
        if (s == null) {
            return NO_VALUE;
        }

        boolean negative = false;
        int len = s.length();
        if (len == 0) {
            return NO_VALUE;
        }

        long limit = -Long.MAX_VALUE;
        int radix = 10;

        int i = 0;
        char firstChar = s.charAt(0);
        if (firstChar < '0') { // Possible leading "+" or "-"
            if (firstChar == '-') {
                negative = true;
                limit = Long.MIN_VALUE;
            } else if (firstChar != '+') {
                return NO_VALUE;
            }

            if (len == 1) { // Cannot have lone "+" or "-"
                return NO_VALUE;
            }
            i++;
        }
        long multmin = limit / radix;
        long result = 0;
        while (i < len) {
            // Accumulating negatively avoids surprises near MAX_VALUE
            int digit = Character.digit(s.charAt(i++), radix);
            if (digit < 0 || result < multmin) {
                return NO_VALUE;
            }
            result *= radix;
            if (result < limit + digit) {
                return NO_VALUE;
            }
            result -= digit;
        }
        return longValue(negative ? result : -result);
    }

    private static Seq<Expression> asList(String stringList) {
        var expression = parser(stringList).expression();
        if (expression instanceof ListLiteral listLiteral) {
            return listLiteral.expressions();
        } else if (expression instanceof Literal literal) {
            throw VectorUtils.invalidVectorType(ValueUtils.of(literal.value()));
        } else {
            throw invalidVectorType(expression);
        }
    }

    private static long asLong(Expression expression) {
        Number number = asNumber(expression);
        if (number instanceof Float || number instanceof Double) {
            return safeCastToLong(number.doubleValue());
        } else {
            return number.longValue();
        }
    }

    private static Number asNumber(Expression expression) {
        if (expression instanceof NumberLiteral numberLiteral) {
            return numberLiteral.value();
        } else if (expression instanceof Literal literal) {
            throw VectorUtils.invalidVectorType(ValueUtils.of(literal.value()));
        } else {
            throw invalidVectorType(expression);
        }
    }

    private static AstParser parser(String expression) {
        List<SemanticFeature> semanticFeatures = List.of();
        return parserFactory.apply(
                expression,
                new Neo4jCypherExceptionFactory(expression, Option.empty()),
                Option.empty(),
                CollectionHasAsScala(semanticFeatures).asScala().toSeq());
    }

    private static CypherTypeException invalidVectorType(Expression badInput) {
        return CypherTypeException.functionArgumentWrongType(
                "Invalid input for function 'vector': Expected a NUMBER, got: " + badInput.asCanonicalStringVal(),
                "vector",
                badInput.asCanonicalStringVal(),
                List.of("INTEGER", "FLOAT"),
                "ANY");
    }
}
