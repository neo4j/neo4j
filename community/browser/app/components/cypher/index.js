function wordRegexp(words) {
    return new RegExp("^(?:" + words.join("|") + ")$", "i");
}

CodeMirror.defineMode("cypher", function (config) {
    var indentUnit = config.indentUnit;
    var curPunc;

    var funcs = wordRegexp(["str", "min", "labels", "max", "type", "lower", "upper", "length", "type", "id",
        "coalesce", "head", "last", "nodes", "relationships", "extract", "filter", "tail", "range",
        "reduce", "abs", "round", "sqrt", "sign", "replace", "substring", "left", "right", "ltrim",
        "rtrim", "trim", "collect", "distinct"]);
    var preds = wordRegexp(["all", "any", "none", "single", "not", "in", "has", "and", "or"]);
    var keywords = wordRegexp(["start", "merge", "using", "on create", "on match", "match", "index on", "drop", "where", "with", "limit", "skip", "order", "by",
        "return", "create", "delete", "set", "unique"]);
    var operatorChars = /[*+\-<>=&|~]/;

    function tokenBase(stream, state) {
        var ch = stream.next();
        curPunc = null;
        if (ch == '"' || ch == "'") {
            stream.match(/.+?["']/);
            return "string";
        }
        if (/[{}\(\),\.;\[\]]/.test(ch)) {
            curPunc = ch;
            return "node";
        } else if (ch == "//") {
            stream.skipToEnd();
            return "comment";
        } else if (operatorChars.test(ch)) {
            stream.eatWhile(operatorChars);
            return null;
        } else {
            stream.eatWhile(/[_\w\d]/);
            if (stream.eat(":")) {
                stream.eatWhile(/[\w\d_\-]/);
                return "atom";
            }
            var word = stream.current(), type;
            if (funcs.test(word)) {
                return "builtin";
            }
            if (preds.test(word)) {
                return "def";
            }
            else if (keywords.test(word)) {
                return "keyword";
            }
            else {
                return "variable";
            }
        }
    }

    function tokenLiteral(quote) {
        return function (stream, state) {
            var escaped = false, ch;
            while ((ch = stream.next()) != null) {
                if (ch == quote && !escaped) {
                    state.tokenize = tokenBase;
                    break;
                }
                escaped = !escaped && ch == "\\";
            }
            return "string";
        };
    }

    function pushContext(state, type, col) {
        state.context = {prev: state.context, indent: state.indent, col: col, type: type};
    }

    function popContext(state) {
        state.indent = state.context.indent;
        state.context = state.context.prev;
    }

    return {
        startState: function (base) {
            return {tokenize: tokenBase,
                context: null,
                indent: 0,
                col: 0};
        },

        token: function (stream, state) {
            if (stream.sol()) {
                if (state.context && state.context.align == null) {
                    state.context.align = false;
                }
                state.indent = stream.indentation();
            }
            if (stream.eatSpace()) {
                return null;
            }

            var style = state.tokenize(stream, state);

            if (style != "comment" && state.context && state.context.align == null && state.context.type != "pattern") {
                state.context.align = true;
            }

            if (curPunc == "(") {
                pushContext(state, ")", stream.column());
            } else if (curPunc == "[") {
                pushContext(state, "]", stream.column());
            } else if (curPunc == "{") {
                pushContext(state, "}", stream.column());
            }
            else if (/[\]\}\)]/.test(curPunc)) {
                while (state.context && state.context.type == "pattern") {
                    popContext(state);
                }
                if (state.context && curPunc == state.context.type) {
                    popContext(state);
                }
            } else if (curPunc == "." && state.context && state.context.type == "pattern") {
                popContext(state);
            } else if (/atom|string|variable/.test(style) && state.context) {
                if (/[\}\]]/.test(state.context.type)) {
                    pushContext(state, "pattern", stream.column());
                }
                else if (state.context.type == "pattern" && !state.context.align) {
                    state.context.align = true;
                    state.context.col = stream.column();
                }
            }

            return style;
        },

        indent: function (state, textAfter) {
            var firstChar = textAfter && textAfter.charAt(0);
            var context = state.context;
            if (/[\]\}]/.test(firstChar))
                while (context && context.type == "pattern") context = context.prev;

            var closing = context && firstChar == context.type;
            if (!context)
                return 0;
            else if (context.type == "keywords")
                return newlineAndIndent;
            else if (context.align)
                return context.col + (closing ? 0 : 1);
            else
                return context.indent + (closing ? 0 : indentUnit);
        }
    };
});

CodeMirror.modeExtensions["cypher"] = {
    autoFormatLineBreaks: function (text) {
        var lines = text.split("\n");
        var reProcessedPortion = /\s+\b(return|where|order by|match|with|skip|limit|create|delete|set)\b\s/gi;
        for (var i = 0; i < lines.length; i++) {
            lines[i] = lines[i].replace(reProcessedPortion, " \n$1 ").trim();
        }
        return lines.join("\n");
    }
};

CodeMirror.defineMIME("application/x-cypher-query", "cypher");
