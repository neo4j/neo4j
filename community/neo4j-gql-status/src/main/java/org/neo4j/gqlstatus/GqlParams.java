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
        // name(Processor       // Description[ | Example]
        action(new VERBATIM()), // Freeform description of some "action"
        alias(new IDENT()), // Alias name
        alias1(new IDENT()),
        alias2(new IDENT()),
        alloc(new IDENT()), // Allocator name
        allocType(new STRLIT()), // Allocation type
        arg(new STRLIT()), // Procedure or function argument, for example, `database`, `pause`, `dryrun`, etc.
        auth(new IDENT()), // Auth provider name
        authRule(new IDENT()), // Auth rule name
        boltServerState(new STRLIT()), // Bolt server state
        cause(new VERBATIM()), // Freeform cause
        cfgSetting(new VERBATIM()), // Configuration setting key | https.enable
        clause(new UPPER().withInner(new VERBATIM())), // Clause
        cmd(new STRLIT()), // Command | 'DROP DATABASE'
        component(new STRLIT()), // Component name
        constr(new IDENT()), // Constraint name
        constrDescrOrName(new STRLIT()), // Constraint descriptor or name
        constrDescrOrName1(new STRLIT()),
        constrDescrOrName2(new STRLIT()),
        context(new VERBATIM()), // Freeform description of some "context"
        coordinates(new COORDINATES()), // Coordinates
        crs(new VERBATIM()), // Coordinate reference system | WGS8
        db(new IDENT()), // Database name | myDb
        db1(new IDENT()),
        db2(new IDENT()),
        db3(new IDENT()),
        edition(new VERBATIM()), // Freeform edition description
        endpointType(new VERBATIM()), // One of 'start', 'end'
        entityId(new VERBATIM()), // Id of a node or relationship
        entityType(new VERBATIM()), // One of 'node', 'relationship'
        expr(new STRLIT()), // Cypher expression | 1 + n.pro
        exprType(new VERBATIM()), // Freeform expression type
        feat(new VERBATIM()), // Freeform feature description | World domination
        feat1(new VERBATIM()),
        feat2(new VERBATIM()),
        field(new IDENT()), // Field identifier
        format(new STRLIT()), // Duration format
        fun(new CALLABLE_IDENT()), // Function name
        funClass(new IDENT()), // Function implementation class name
        funType(new VERBATIM()), // Function type, e.g. non-deterministic or aggregate
        graph(new IDENT()), // Graph name | myGrap
        graphTypeDependence1(new VERBATIM()), // GraphTypeDependence | independent
        graphTypeDependence2(new VERBATIM()),
        graphTypeElement1(new VERBATIM()),
        graphTypeElement2(new VERBATIM()),
        graphTypeReference(new STRLIT()), // Graph type reference
        graphTypeOperation(new VERBATIM()), // One of SET, ADD, DROP, ALTER
        groupingConstructs(
                new VERBATIM()), // Constructs that establish a grouping, e.g. `DISTINCT`, an aggregation, or a `GROUP
        // BY` clause
        hint(new VERBATIM()), // Freeform description of some "hint"
        ident(new IDENT()), // Generic identifier
        idx(new IDENT()), // Index name
        idxDescr(new STRLIT()), // Index descriptor
        idxDescrOrName(new STRLIT()), // Index descriptor or name
        idxOrConstr(new IDENT()), // Index or constraint name
        idxOrConstrPat(new STRLIT()), // Index or constraint pattern
        idxType(new VERBATIM()), // Index type (e.g,, text, vector, ...)
        idxType1(new VERBATIM()), // Index type (e.g,, text, vector, ...)
        idxType2(new VERBATIM()), // Index type (e.g,, text, vector, ...)
        input(new STRLIT()), // Piece of input
        input1(new STRLIT()),
        input2(new STRLIT()),
        item(new VERBATIM()), // Freeform description of some "item"
        keyword(new STRLIT()), // Pattern keyword
        label(new IDENT()), // Label name | Person
        labelExpr(new STRLIT()), // Label expression | Person&Human
        lower(new VERBATIM()), // Lower bound e.g. number out of range (StringParam to handle Durations)
        mapKey(new STRLIT()), // Map key
        matchMode(new VERBATIM()), // A GPM match mode
        msg(new VERBATIM()), // Freeform message | Howdy, Partner
        msgTitle(new VERBATIM()), // Freeform message title
        namespace(new IDENT()), // Namespace
        operation(new STRLIT()), // Operation
        option(new STRLIT()), // Option name
        option1(new STRLIT()),
        option2(new STRLIT()),
        param(new PARAM()), // Parameter name | $para
        param1(new PARAM()),
        param2(new PARAM()),
        pat(new STRLIT()), // Pattern | '(:Person)'
        pathMode(new VERBATIM()), // A GPM path mode
        port(new IDENT()), // Port name
        pred(new STRLIT()), // Predicate | 'x < 3'
        preparserInput1(new STRLIT()), // Piece of preparser input
        preparserInput2(new STRLIT()),
        proc(new CALLABLE_IDENT()), // Procedure name | launchRocket
        procClass(new IDENT()), // Procedure implementation class name
        procExeMode(new STRLIT()), // Procedure execution mode
        procField(new IDENT()), // Procedure implementation class field name | someField
        procFieldType(new STRLIT()), // Procedure implementation class field type
        procFun(new CALLABLE_IDENT()), // Procedure or function name or id
        procMethod(new IDENT()), // Procedure implementation class method name
        procParam(new IDENT()), // Procedure parameter name
        procParamFmt(new VERBATIM()), // Freeform procedure parameter format
        propKey(new IDENT()), // Property key name | name
        query(new STRLIT()), // Query string extract | MATCH (n) WHERE n.prop...
        relType(new IDENT()), // Relationship type name | KNOWS
        replacement(new STRLIT()), // Replacement
        role(new IDENT()), // Role name
        routingPolicy(new STRLIT()), // Routing policy
        runtime(new STRLIT()), // Cypher runtime name
        schemaDescr(new STRLIT()), // Schema descriptor
        schemaDescrType(new VERBATIM()), // type of schema descriptor
        selector(new VERBATIM()), // A GPM path selector
        selectorOrPathMode(new VERBATIM()), // A GPM path selector or GPM path mode
        selectorType1(new STRLIT()), // Selector type
        selectorType2(new STRLIT()),
        server(new STRLIT()), // Server | 'example.com
        serverAddress(new STRLIT()), // Server address | localhost:1024
        serverType(new STRLIT()), // Server type
        sig(new VERBATIM()), // Procedure or function signature
        storeFormat(new VERBATIM()), // Store format name. One of "aligned", "block", "standard", "high_limit".
        syntax(new IDENT()), // Freeform syntax or keyword
        temporal(new TEMPORAL()), // Temporal value
        temporal1(new TEMPORAL()),
        temporal2(new TEMPORAL()),
        timeUnit(new VERBATIM()), // Common time unit name
        token(new STRLIT()), // Token name
        token1(new STRLIT()),
        token2(new STRLIT()),
        tokenType(new VERBATIM()), // Token type
        tokenType1(new VERBATIM()),
        tokenType2(new VERBATIM()),
        transactionId(new STRLIT()), // Transaction id
        transactionId1(new STRLIT()),
        transactionId2(new STRLIT()),
        typeDescription(new VERBATIM()), // Freeform description of a type e.g. 'a list'
        upper(new VERBATIM()), // Upper bound e.g. number out of range (StringParam to handle Durations)
        url(new VERBATIM()), // URL
        user(new IDENT()), // User name
        value(new VAL()), // Value
        valueType(new VALTYPE()), // Value type
        variable(new IDENT()), // Variable name
        variable1(new IDENT()), //
        variable2(new IDENT()); //
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
        boltMsgLenLimit(new NUM()), // Bolt message length limit
        bytes(new NUM()),
        bytes1(new NUM()),
        bytes2(new NUM()),
        count(new NONNEG()), // Amount
        count1(new NONNEG()),
        count2(new NONNEG()),
        count3(new NONNEG()),
        countAllocs(new NUM()), // Desired number of servers to use
        countSeeders(new NUM()), // Number of seeding servers
        dim1(new NONNEG()), // Number representing index dimensionality
        dim2(new NONNEG()),
        entityId(new NUM()), // Id of a node or relationship
        entityId1(new NUM()),
        entityId2(new NUM()),
        lower(new NUM()), // Lower bound
        pos(new NUM()), // A position (e.g., in a sequence)
        timeAmount(new NUM()), // Integral amount of some time unit
        tokenId(new NUM()), // Token id
        upper(new NUM()), // Upper bound
        value(new VAL()), // Value
        version1(new NUM()), // A version, for example, `25` in `CYPHER 25`.
        version2(new NUM());

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
        aliasList(new NELIST().withInner(StringParam.alias.processor)), // Comma-separated list of alias names
        argList(new NELIST()
                .withInner(
                        StringParam.arg
                                .processor)), // A list of procedure or function arguments, for example, `edition`,
        // `name` and `versions`
        clauseList(new NELIST().withInner(StringParam.clause.processor)), // Comma-separated list of clauses
        dbList(new NELIST().withInner(StringParam.db.processor)), // Comma-separated list of database names
        hintList(new NELIST()
                .withInner(
                        StringParam.hint.processor)), // Comma-separated list of free form descriptions of some "hints"
        inputList(new NELIST().withInner(StringParam.input.processor)), // Comma-separated list of "inputs"
        labelList(new NELIST()
                .withInner(StringParam.label.processor)), // Comma-separated list of label names | Person, Human
        mapKeyList(new NELIST().withInner(StringParam.mapKey.processor)), // Comma-separated list of keys
        namespaceList(new NELIST().withInner(StringParam.namespace.processor)), // Comma-separated list of namespaces
        optionList(new NELIST().withInner(StringParam.option.processor)), // Comma-separated list of option names
        paramList(new NELIST().withInner(StringParam.param.processor)), // Parameter list | $name, $age
        paramList1(new NELIST().withInner(StringParam.param.processor)),
        paramList2(new NELIST().withInner(StringParam.param.processor)),
        pathModes(new NELIST().withInner(StringParam.pathMode.processor)), // Comma-separated list of GPM path modes
        portList(new NELIST().withInner(StringParam.port.processor)), // Comma-separated list of port names
        predList(new NELIST()
                .withInner(StringParam.pred.processor)), // Comma-separated list of predicates | 'x < 3', 'y > 4'
        propKeyList(
                new NELIST().withInner(StringParam.propKey.processor)), // Comma-separated list of property key names
        reasonList(
                new NELIST().withInner(StringParam.value.processor)), // Comma-separated list of reasons of the failure
        serverList(new NELIST()
                .withInner(StringParam.server.processor)), // Comma-separated list of servers | 'a.com', 'b.com'
        valueList(new NELIST().withInner(StringParam.value.processor)), // Comma-separated list of values
        valueTypeList(new NELIST().withInner(StringParam.valueType.processor)),
        variableList(new NELIST().withInner(StringParam.variable.processor)); // Comma-separated list of values

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

    public static class BOOLEAN extends Processor {}

    public static class NELIST extends ListProcessor {}
}
