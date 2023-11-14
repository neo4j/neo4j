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
package org.neo4j.codegen.source;

import java.util.Deque;
import java.util.LinkedList;
import java.util.function.Consumer;
import org.apache.commons.text.StringEscapeUtils;
import org.neo4j.codegen.Expression;
import org.neo4j.codegen.ExpressionVisitor;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.LocalVariable;
import org.neo4j.codegen.MethodReference;
import org.neo4j.codegen.MethodWriter;
import org.neo4j.codegen.Parameter;
import org.neo4j.codegen.TypeReference;

class JavaSourceMethodWriter implements MethodWriter, ExpressionVisitor {
    private static final Runnable BOTTOM = () -> {
        throw new IllegalStateException("Popped too many levels!");
    };
    private static final Runnable LEVEL = () -> {};
    private static final String INDENTATION = "    ";
    private final StringBuilder target;
    private final JavaSourceClassWriter classSourceWriter;
    private final boolean isStatic;
    private final Deque<Runnable> levels = new LinkedList<>();

    JavaSourceMethodWriter(StringBuilder target, JavaSourceClassWriter classSourceWriter, boolean isStatic) {
        this.target = target;
        this.classSourceWriter = classSourceWriter;
        this.isStatic = isStatic;
        this.levels.push(BOTTOM);
        this.levels.push(LEVEL);
    }

    private StringBuilder indent() {
        for (int level = this.levels.size(); level-- > 0; ) {
            target.append(INDENTATION);
        }
        return target;
    }

    private StringBuilder append(CharSequence text) {
        return target.append(text);
    }

    @Override
    public boolean isStatic() {
        return isStatic;
    }

    @Override
    public void done() {
        if (levels.size() != 1) {
            throw new IllegalStateException("unbalanced blocks!");
        }
        classSourceWriter.append(target);
    }

    @Override
    public void expression(Expression expression) {
        if (expression == Expression.EMPTY) {
            return;
        }
        indent();
        expression.accept(this);
        target.append(";\n");
    }

    @Override
    public void put(Expression target, FieldReference field, Expression value) {
        indent();
        target.accept(this);
        append(".");
        append(field.name());
        append(" = ");
        value.accept(this);
        append(";\n");
    }

    @Override
    public void putStatic(FieldReference field, Expression value) {
        indent();
        append(field.owner().fullName());
        append(".");
        append(field.name());
        append(" = ");
        value.accept(this);
        append(";\n");
    }

    @Override
    public void returns() {
        indent().append("return;\n");
    }

    @Override
    public void returns(Expression value) {
        indent().append("return ");
        value.accept(this);
        append(";\n");
    }

    @Override
    public void continues() {
        indent().append("continue;\n");
    }

    @Override
    public void breaks(String labelName) {
        indent().append("break " + labelName + ";\n");
    }

    @Override
    public void declare(LocalVariable local) {
        indent().append(local.type().fullName())
                .append(' ')
                .append(local.name())
                .append(";\n");
    }

    @Override
    public void assignVariableInScope(LocalVariable local, Expression value) {
        indent().append(local.name()).append(" = ");
        value.accept(this);
        append(";\n");
    }

    @Override
    public void assign(LocalVariable variable, Expression value) {
        indent().append(variable.type().fullName())
                .append(' ')
                .append(variable.name())
                .append(" = ");
        value.accept(this);
        append(";\n");
    }

    @Override
    public void beginWhile(Expression test, String labelName) {
        if (labelName != null && !labelName.isEmpty()) {
            indent().append(labelName + ":\n");
        }
        indent().append("while( ");
        test.accept(this);
        append(" )\n");
        indent().append("{\n");
        levels.push(LEVEL);
    }

    @Override
    public void beginIf(Expression test) {
        indent().append("if ( ");
        test.accept(this);
        append(" )\n");
        indent().append("{\n");
        levels.push(LEVEL);
    }

    @Override
    public <T> void ifElseStatement(Expression test, Consumer<T> onTrue, Consumer<T> onFalse, T block) {
        beginIf(test);
        onTrue.accept(block);
        levels.pop();
        indent().append("}\n");
        indent().append("else {\n");
        levels.push(LEVEL);
        onFalse.accept(block);
        levels.pop();
        indent().append("}\n");
    }

    @Override
    public void beginBlock() {
        indent().append("{\n");
        levels.push(LEVEL);
    }

    @Override
    public void beginTry(Parameter exception) {
        indent().append("try\n");
        indent().append("{\n");
        levels.push(LEVEL);
    }

    public void catchIt() {}

    @Override
    public void beginCatch(LocalVariable exception) {
        indent().append("catch ( ")
                .append(exception.type().fullName())
                .append(' ')
                .append(exception.name())
                .append(" )\n");
        indent().append("{\n");
        levels.push(LEVEL);
    }

    @Override
    public void throwException(Expression exception) {
        indent().append("throw ");
        exception.accept(this);
        append(";\n");
    }

    @Override
    public void endBlock() {
        Runnable action = levels.pop();
        indent().append("}\n");
        action.run();
    }

    @Override
    public void invoke(Expression target, MethodReference method, Expression[] arguments) {
        target.accept(this);
        if (!method.isConstructor()) {
            append(".").append(method.name());
        }
        arglist(arguments);
    }

    @Override
    public void invoke(MethodReference method, Expression[] arguments) {
        append(method.owner().fullName()).append('.').append(method.name());
        arglist(arguments);
    }

    private void arglist(Expression[] arguments) {
        append("(");
        String sep = " ";
        for (Expression argument : arguments) {
            append(sep);
            argument.accept(this);
            sep = ", ";
        }
        if (sep.length() > 1) {
            append(" ");
        }
        append(")");
    }

    @Override
    public void load(LocalVariable variable) {
        append(variable.name());
    }

    @Override
    public void arrayLoad(Expression array, Expression index) {
        array.accept(this);
        append("[");
        index.accept(this);
        append("]");
    }

    @Override
    public void arraySet(Expression array, Expression index, Expression value) {
        array.accept(this);
        append("[");
        index.accept(this);
        append("] = ");
        value.accept(this);
    }

    @Override
    public void arrayLength(Expression array) {
        array.accept(this);
        append(".length");
    }

    @Override
    public void getField(Expression target, FieldReference field) {
        target.accept(this);
        append(".").append(field.name());
    }

    @Override
    public void constant(Object value) {
        if (value == null) {
            append("null");
        } else if (value instanceof String) {
            append("\"").append(StringEscapeUtils.escapeJava((String) value)).append('"');
        } else if (value instanceof Integer) {
            append(value.toString());
        } else if (value instanceof Long) {
            append(value.toString()).append('L');
        } else if (value instanceof Double doubleValue) {
            if (Double.isNaN(doubleValue)) {
                append("Double.NaN");
            } else if (doubleValue == Double.POSITIVE_INFINITY) {
                append("Double.POSITIVE_INFINITY");
            } else if (doubleValue == Double.NEGATIVE_INFINITY) {
                append("Double.NEGATIVE_INFINITY");
            } else {
                append(value.toString());
            }
        } else if (value instanceof Boolean) {
            append(value.toString());
        } else {
            throw new UnsupportedOperationException(value.getClass() + " constants");
        }
    }

    @Override
    public void getStatic(FieldReference field) {
        append(field.owner().fullName()).append('.').append(field.name());
    }

    @Override
    public void loadThis(String sourceName) {
        append(sourceName);
    }

    @Override
    public void newInstance(TypeReference type) {
        append("new ").append(type.fullName());
    }

    @Override
    public void not(Expression expression) {
        append("!( ");
        expression.accept(this);
        append(" )");
    }

    @Override
    public void ternary(Expression test, Expression onTrue, Expression onFalse) {
        append("((");
        test.accept(this);
        append(") ? (");
        onTrue.accept(this);
        append(") : (");
        onFalse.accept(this);
        append("))");
    }

    @Override
    public void equal(Expression lhs, Expression rhs) {
        binaryOperation(lhs, rhs, " == ");
    }

    @Override
    public void notEqual(Expression lhs, Expression rhs) {
        binaryOperation(lhs, rhs, " != ");
    }

    @Override
    public void isNull(Expression expression) {
        expression.accept(this);
        append(" == null");
    }

    @Override
    public void notNull(Expression expression) {
        expression.accept(this);
        append(" != null");
    }

    @Override
    public void or(Expression... expressions) {
        boolOp(expressions, " || ");
    }

    @Override
    public void and(Expression... expressions) {
        boolOp(expressions, " && ");
    }

    private void boolOp(Expression[] expressions, String op) {
        String sep = "";
        for (Expression expression : expressions) {
            append(sep);
            append("(");
            expression.accept(this);
            append(")");
            sep = op;
        }
    }

    @Override
    public void add(Expression lhs, Expression rhs) {
        binaryOperation(lhs, rhs, " + ");
    }

    @Override
    public void gt(Expression lhs, Expression rhs) {
        binaryOperation(lhs, rhs, " > ");
    }

    @Override
    public void gte(Expression lhs, Expression rhs) {
        binaryOperation(lhs, rhs, " >= ");
    }

    @Override
    public void lt(Expression lhs, Expression rhs) {
        binaryOperation(lhs, rhs, " < ");
    }

    @Override
    public void lte(Expression lhs, Expression rhs) {
        binaryOperation(lhs, rhs, " <= ");
    }

    @Override
    public void subtract(Expression lhs, Expression rhs) {
        binaryOperation(lhs, rhs, " - ");
    }

    @Override
    public void multiply(Expression lhs, Expression rhs) {
        binaryOperation(lhs, rhs, " * ");
    }

    private void div(Expression lhs, Expression rhs) {
        binaryOperation(lhs, rhs, " / ");
    }

    @Override
    public void cast(TypeReference type, Expression expression) {
        if (!type.equals(expression.type())) {
            append("(");
            append("(").append(type.fullName()).append(") ");
            append("(");
            expression.accept(this);
            append(")");
            append(")");
        } else {
            expression.accept(this);
        }
    }

    @Override
    public void instanceOf(TypeReference type, Expression expression) {
        expression.accept(this);
        append(" instanceof ").append(type.fullName());
    }

    @Override
    public void newInitializedArray(TypeReference type, Expression... constants) {
        append("new ").append(type.fullName()).append("[]{");
        String sep = "";
        for (Expression constant : constants) {
            append(sep);
            constant.accept(this);
            sep = ", ";
        }
        append("}");
    }

    @Override
    public void newArray(TypeReference type, int size) {
        if (type.isArray()) {
            append("new ")
                    .append(type.baseName())
                    .append('[')
                    .append(size)
                    .append(']')
                    .append("[]".repeat(type.arrayDepth()));
        } else {
            append("new ").append(type.fullName()).append('[').append(size).append(']');
        }
    }

    @Override
    public void newArray(TypeReference type, Expression size) {
        if (type.isArray()) {
            append("new ").append(type.baseName()).append('[');
            size.accept(this);
            append("]").append("[]".repeat(type.arrayDepth()));
        } else {
            append("new ").append(type.fullName()).append('[');
            size.accept(this);
            append("]");
        }
    }

    @Override
    public void longToDouble(Expression expression) {
        cast(TypeReference.typeReference(double.class), expression);
    }

    @Override
    public void pop(Expression expression) {
        expression.accept(this);
    }

    @Override
    public void box(Expression expression) {
        // For source code we rely on autoboxing
        append("(/*box*/ ");
        expression.accept(this);
        append(")");
    }

    @Override
    public void unbox(Expression expression) {
        // For source code we rely on autoboxing
        expression.accept(this);
    }

    private void binaryOperation(Expression lhs, Expression rhs, String operator) {
        append("(");
        lhs.accept(this);
        append(")");
        append(operator);
        append("(");
        rhs.accept(this);
        append(")");
    }
}
