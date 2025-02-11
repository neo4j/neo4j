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
package org.neo4j.gqlstatus;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class GqlParams {

    // visible for testing
    public static final String substitution = "{ %s }";

    public interface GqlParam {
        String name();

        String process(Object s);

        default String toParamFormat() {
            return "$" + this.name();
        }
    }

    public enum StringParam implements GqlParam {
        action(new VERBATIM()),
        alias(new IDENT()),
        alias1(new IDENT()),
        alias2(new IDENT()),
        alloc(new IDENT()),
        allocType(new STRLIT()),
        auth(new IDENT()),
        boltMsgType(new STRLIT()),
        boltServerState(new STRLIT()),
        cause(new VERBATIM()),
        cfgSetting(new VERBATIM()),
        changeIdent(new IDENT()),
        characterRange(new CHAR_RANGE()),
        clause(new UPPER().withInner(new VERBATIM())),
        cmd(new STRLIT()),
        component(new STRLIT()),
        constr(new IDENT()),
        constrDescrOrName(new STRLIT()),
        context(new VERBATIM()),
        coordinates(new COORDINATES()),
        crs(new VERBATIM()),
        db(new IDENT()),
        db1(new IDENT()),
        db2(new IDENT()),
        db3(new IDENT()),
        edition(new VERBATIM()),
        entityId1(new STRLIT()),
        entityId2(new STRLIT()),
        entityType(new VERBATIM()),
        expr(new STRLIT()),
        exprType(new VERBATIM()),
        feat(new VERBATIM()),
        feat1(new VERBATIM()),
        feat2(new VERBATIM()),
        field(new IDENT()),
        format(new STRLIT()),
        fun(new CALLABLE_IDENT()),
        graph(new IDENT()),
        hint(new VERBATIM()),
        ident(new IDENT()),
        idx(new IDENT()),
        idxDescr(new STRLIT()),
        idxDescrOrName(new STRLIT()),
        idxOrConstr(new IDENT()),
        idxOrConstrPat(new STRLIT()),
        idxType(new VERBATIM()),
        input(new STRLIT()),
        input1(new STRLIT()),
        input2(new STRLIT()),
        item(new VERBATIM()),
        keyword(new STRLIT()),
        label(new IDENT()),
        labelExpr(new STRLIT()),
        mapKey(new STRLIT()),
        matchMode(new VERBATIM()),
        msg(new VERBATIM()),
        msgTitle(new VERBATIM()),
        namespace(new IDENT()),
        operation(new STRLIT()),
        option(new STRLIT()),
        option1(new STRLIT()),
        option2(new STRLIT()),
        param(new PARAM()),
        param1(new PARAM()),
        param2(new PARAM()),
        pat(new STRLIT()),
        port(new IDENT()),
        pred(new STRLIT()),
        preparserInput(new STRLIT()),
        preparserInput1(new STRLIT()),
        preparserInput2(new STRLIT()),
        proc(new CALLABLE_IDENT()),
        procClass(new IDENT()),
        procExeMode(new STRLIT()),
        procField(new IDENT()),
        procFieldType(new STRLIT()),
        procFun(new CALLABLE_IDENT()),
        procMethod(new IDENT()),
        procParam(new IDENT()),
        procParamFmt(new VERBATIM()),
        propKey(new IDENT()),
        query(new STRLIT()),
        relType(new IDENT()),
        replacement(new STRLIT()),
        role(new IDENT()),
        routingPolicy(new STRLIT()),
        runtime(new STRLIT()),
        schemaDescr(new STRLIT()),
        selector(new VERBATIM()),
        selectorType(new STRLIT()),
        selectorType1(new STRLIT()),
        selectorType2(new STRLIT()),
        server(new STRLIT()),
        serverType(new STRLIT()),
        sig(new VERBATIM()),
        syntax(new IDENT()),
        temporal(new TEMPORAL()),
        temporal1(new TEMPORAL()),
        temporal2(new TEMPORAL()),
        timeUnit(new IDENT()),
        token(new STRLIT()),
        tokenId(new STRLIT()),
        tokenType(new VERBATIM()),
        transactionId(new STRLIT()),
        transactionId1(new STRLIT()),
        transactionId2(new STRLIT()),
        url(new VERBATIM()),
        user(new IDENT()),
        value(new VAL()),
        valueType(new VALTYPE()),
        variable(new IDENT());

        public final Processor processor;

        @Override
        public String process(Object s) {
            return processor.process(s);
        }

        StringParam(Processor proc) {
            this.processor = proc;
        }
    }

    public enum NumberParam implements GqlParam {
        boltMsgLenLimit(new NUM()),
        count(new NONNEG()),
        count1(new NONNEG()),
        count2(new NONNEG()),
        countAllocs(new NUM()),
        countSeeders(new NUM()),
        dim1(new NONNEG()),
        dim2(new NONNEG()),
        entityId(new STRLIT()),
        lower(new NUM()),
        pos(new NUM()),
        timeAmount(new NUM()),
        upper(new NUM()),
        value(new VAL());

        public final Processor processor;

        @Override
        public String process(Object s) {
            return processor.process(s);
        }

        NumberParam(Processor proc) {
            this.processor = proc;
        }
    }

    public enum ListParam implements GqlParam, HasJoinStyle {
        characterRangeList(new NELIST().withInner(StringParam.characterRange.processor)),
        hintList(new NELIST().withInner(StringParam.hint.processor)),
        inputList(new NELIST().withInner(StringParam.input.processor)),
        labelList(new NELIST().withInner(StringParam.label.processor)),
        mapKeyList(new NELIST().withInner(StringParam.mapKey.processor)),
        namespaceList(new NELIST().withInner(StringParam.namespace.processor)),
        optionList(new NELIST().withInner(StringParam.option.processor)),
        paramList(new NELIST().withInner(StringParam.param.processor)),
        portList(new NELIST().withInner(StringParam.port.processor)),
        predList(new NELIST().withInner(StringParam.pred.processor)),
        propKeyList(new NELIST().withInner(StringParam.propKey.processor)),
        serverList(new NELIST().withInner(StringParam.server.processor)),
        valueList(new NELIST().withInner(StringParam.value.processor)),
        valueTypeList(new NELIST().withInner(StringParam.valueType.processor)),
        variableList(new NELIST().withInner(StringParam.variable.processor));

        public final ListProcessor processor;

        @Override
        public String process(Object s) {
            return processor.process(s);
        }

        @Override
        public String process(List<?> list, JoinStyle joinStyle) {
            return processor.process(list, joinStyle);
        }

        ListParam(ListProcessor proc) {
            this.processor = proc;
        }
    }

    public enum BooleanParam implements GqlParam {
        value(new VAL());

        public final Processor processor;

        @Override
        public String process(Object s) {
            return processor.process(s);
        }

        BooleanParam(Processor proc) {
            this.processor = proc;
        }
    }

    public interface SpecialRule {}

    public enum JoinStyle implements SpecialRule {
        ANDED,
        ORED,
        COMMAD;
    }

    public abstract static class Processor {
        public Processor inner;

        public String process(Object o) {
            return String.valueOf(o);
        }

        public Processor withInner(Processor p) {
            this.inner = p;
            return this;
        }
    }

    public abstract static class ListProcessor extends Processor implements HasJoinStyle {

        private static String formatList(List<?> list, SpecialRule joinStyle) {
            if (joinStyle == null) return commadFormat(list);
            if (joinStyle.equals(JoinStyle.ANDED)) {
                return andedFormat(list);
            } else if (joinStyle.equals(JoinStyle.ORED)) {
                return oredFormat(list);
            } else {
                return commadFormat(list);
            }
        }

        private static String oredFormat(List<?> list) {
            if (list.isEmpty()) return "";
            else if (list.size() == 1) return String.valueOf(list.get(0));
            StringBuilder sb = initialCommas(list);
            sb.append(" or ").append(String.valueOf(list.get(list.size() - 1)));
            return sb.toString();
        }

        private static String andedFormat(List<?> list) {
            if (list.isEmpty()) return "";
            else if (list.size() == 1) return String.valueOf(list.get(0));
            StringBuilder sb = initialCommas(list);
            sb.append(" and ").append(String.valueOf(list.get(list.size() - 1)));
            return sb.toString();
        }

        private static String commadFormat(List<?> list) {
            if (list.isEmpty()) return "";
            else if (list.size() == 1) return String.valueOf(list.get(0));
            StringBuilder sb = initialCommas(list);
            sb.append(", ").append(String.valueOf(list.get(list.size() - 1)));
            return joinListWithConjunction(list, ",");
        }

        private static String joinListWithConjunction(List<?> list, String conjunction) {
            if (list.isEmpty()) return "";
            if (list.size() == 1) return String.valueOf(list.get(0));

            StringBuilder sb = new StringBuilder();
            sb.append(list.get(0));

            for (int i = 1; i < list.size() - 1; i++) {
                sb.append(", ").append(list.get(i));
            }
            sb.append(conjunction).append(" ").append(list.get(list.size() - 1));

            return sb.toString();
        }

        private static StringBuilder initialCommas(List<?> list) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.valueOf(list.get(0)));
            for (int i = 1; i < list.size() - 1; i++) {
                sb.append(", ").append(String.valueOf(list.get(i)));
            }
            return sb;
        }

        private static String listProcess(List<?> param, SpecialRule joinStyle, Processor inner) {
            if (inner != null) {
                String[] processedParam = new String[param.size()];
                for (int i = 0; i < param.size(); i++) {
                    processedParam[i] = inner.process(param.get(i));
                }
                param = Arrays.stream(processedParam).toList();
            }
            return formatList(param, joinStyle);
        }

        @Override
        public String process(List<?> list, JoinStyle joinStyle) {
            return listProcess(list, joinStyle, inner);
        }

        @Override
        public ListProcessor withInner(Processor p) {
            this.inner = p;
            return this;
        }
    }

    public interface HasJoinStyle {
        String process(List<?> list, JoinStyle joinStyle);
    }

    public static class VERBATIM extends Processor {}

    public static class IDENT extends Processor {
        @Override
        public String process(Object s) {
            return "`" + s + "`";
        }
    }

    public static class CALLABLE_IDENT extends Processor {
        @Override
        public String process(Object s) {
            return s + "()";
        }
    }

    public static class STRLIT extends Processor {
        @Override
        public String process(Object s) {
            return "'" + s + "'";
        }
    }

    public static class PARAM extends Processor {
        @Override
        public String process(Object s) {
            return "$`" + s + "`";
        }
    }

    public static class VAL extends Processor {}

    public static class VALTYPE extends Processor {}

    public static class NUM extends Processor {}

    public static class NONNEG extends Processor {}

    public static class TEMPORAL extends Processor {}

    public static class COORDINATES extends Processor {}

    public static class UPPER extends Processor {
        @Override
        public String process(Object s) {
            if (inner != null) {
                s = inner.process(s);
            }
            return String.valueOf(s).toUpperCase(Locale.ROOT);
        }
    }

    public static class CHAR_RANGE extends Processor {
        @Override
        public String process(Object o) {
            return "`" + o + "`";
        }
    }

    public static class BOOLEAN extends Processor {}

    public static class PELIST extends ListProcessor {}

    public static class NELIST extends ListProcessor {}
}
