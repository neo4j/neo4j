/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
// Generated from /Users/mats/gitRoots/3.0-copy/neo4j/community/cypher/compatibility-suite/src/main/resources/FeatureResults.g4 by ANTLR 4.5.1
package cypher.feature.parser.generated;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class FeatureResultsLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.5.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, INTEGER_LITERAL=17, 
		DECIMAL_LITERAL=18, DIGIT=19, NONZERODIGIT=20, INFINITY=21, FLOAT_LITERAL=22, 
		FLOAT_REPR=23, EXPONENTPART=24, SYMBOLIC_NAME=25, WS=26, IDENTIFIER=27, 
		STRING_LITERAL=28, STRING_BODY=29, ESCAPED_APOSTROPHE=30;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
		"T__9", "T__10", "T__11", "T__12", "T__13", "T__14", "T__15", "INTEGER_LITERAL", 
		"DECIMAL_LITERAL", "DIGIT", "NONZERODIGIT", "INFINITY", "FLOAT_LITERAL", 
		"FLOAT_REPR", "EXPONENTPART", "SYMBOLIC_NAME", "WS", "IDENTIFIER", "STRING_LITERAL", 
		"STRING_BODY", "ESCAPED_APOSTROPHE"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'('", "')'", "'['", "']'", "'<'", "'>'", "'-'", "'->'", "'<-'", 
		"'true'", "'false'", "'null'", "', '", "'{'", "'}'", "':'", null, null, 
		null, null, null, null, null, null, null, "' '", null, null, null, "'\\''"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, "INTEGER_LITERAL", "DECIMAL_LITERAL", "DIGIT", 
		"NONZERODIGIT", "INFINITY", "FLOAT_LITERAL", "FLOAT_REPR", "EXPONENTPART", 
		"SYMBOLIC_NAME", "WS", "IDENTIFIER", "STRING_LITERAL", "STRING_BODY", 
		"ESCAPED_APOSTROPHE"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public FeatureResultsLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "FeatureResults.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2 \u00d3\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\3\2\3\2\3"+
		"\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3\t\3\n\3\n\3\n"+
		"\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r"+
		"\3\16\3\16\3\16\3\17\3\17\3\20\3\20\3\21\3\21\3\22\5\22n\n\22\3\22\3\22"+
		"\3\23\3\23\3\23\7\23u\n\23\f\23\16\23x\13\23\5\23z\n\23\3\24\3\24\5\24"+
		"~\n\24\3\25\3\25\3\26\5\26\u0083\n\26\3\26\3\26\3\26\3\26\3\27\5\27\u008a"+
		"\n\27\3\27\3\27\3\30\6\30\u008f\n\30\r\30\16\30\u0090\3\30\3\30\6\30\u0095"+
		"\n\30\r\30\16\30\u0096\3\30\5\30\u009a\n\30\3\30\3\30\6\30\u009e\n\30"+
		"\r\30\16\30\u009f\3\30\5\30\u00a3\n\30\3\30\3\30\3\30\3\30\6\30\u00a9"+
		"\n\30\r\30\16\30\u00aa\3\30\5\30\u00ae\n\30\5\30\u00b0\n\30\3\31\3\31"+
		"\5\31\u00b4\n\31\3\31\6\31\u00b7\n\31\r\31\16\31\u00b8\3\32\3\32\3\33"+
		"\3\33\3\34\6\34\u00c0\n\34\r\34\16\34\u00c1\3\35\3\35\7\35\u00c6\n\35"+
		"\f\35\16\35\u00c9\13\35\3\35\3\35\3\36\3\36\5\36\u00cf\n\36\3\37\3\37"+
		"\3\37\2\2 \3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33"+
		"\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67"+
		"\359\36;\37= \3\2\7\3\2\63;\4\2GGgg\4\2--//\7\2&&\62;C\\aac|\4\2\2(*\u0201"+
		"\u00e7\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2"+
		"\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3"+
		"\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2"+
		"\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2"+
		"/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2"+
		"\2\2;\3\2\2\2\2=\3\2\2\2\3?\3\2\2\2\5A\3\2\2\2\7C\3\2\2\2\tE\3\2\2\2\13"+
		"G\3\2\2\2\rI\3\2\2\2\17K\3\2\2\2\21M\3\2\2\2\23P\3\2\2\2\25S\3\2\2\2\27"+
		"X\3\2\2\2\31^\3\2\2\2\33c\3\2\2\2\35f\3\2\2\2\37h\3\2\2\2!j\3\2\2\2#m"+
		"\3\2\2\2%y\3\2\2\2\'}\3\2\2\2)\177\3\2\2\2+\u0082\3\2\2\2-\u0089\3\2\2"+
		"\2/\u00af\3\2\2\2\61\u00b1\3\2\2\2\63\u00ba\3\2\2\2\65\u00bc\3\2\2\2\67"+
		"\u00bf\3\2\2\29\u00c3\3\2\2\2;\u00ce\3\2\2\2=\u00d0\3\2\2\2?@\7*\2\2@"+
		"\4\3\2\2\2AB\7+\2\2B\6\3\2\2\2CD\7]\2\2D\b\3\2\2\2EF\7_\2\2F\n\3\2\2\2"+
		"GH\7>\2\2H\f\3\2\2\2IJ\7@\2\2J\16\3\2\2\2KL\7/\2\2L\20\3\2\2\2MN\7/\2"+
		"\2NO\7@\2\2O\22\3\2\2\2PQ\7>\2\2QR\7/\2\2R\24\3\2\2\2ST\7v\2\2TU\7t\2"+
		"\2UV\7w\2\2VW\7g\2\2W\26\3\2\2\2XY\7h\2\2YZ\7c\2\2Z[\7n\2\2[\\\7u\2\2"+
		"\\]\7g\2\2]\30\3\2\2\2^_\7p\2\2_`\7w\2\2`a\7n\2\2ab\7n\2\2b\32\3\2\2\2"+
		"cd\7.\2\2de\7\"\2\2e\34\3\2\2\2fg\7}\2\2g\36\3\2\2\2hi\7\177\2\2i \3\2"+
		"\2\2jk\7<\2\2k\"\3\2\2\2ln\7/\2\2ml\3\2\2\2mn\3\2\2\2no\3\2\2\2op\5%\23"+
		"\2p$\3\2\2\2qz\7\62\2\2rv\5)\25\2su\5\'\24\2ts\3\2\2\2ux\3\2\2\2vt\3\2"+
		"\2\2vw\3\2\2\2wz\3\2\2\2xv\3\2\2\2yq\3\2\2\2yr\3\2\2\2z&\3\2\2\2{~\7\62"+
		"\2\2|~\5)\25\2}{\3\2\2\2}|\3\2\2\2~(\3\2\2\2\177\u0080\t\2\2\2\u0080*"+
		"\3\2\2\2\u0081\u0083\7/\2\2\u0082\u0081\3\2\2\2\u0082\u0083\3\2\2\2\u0083"+
		"\u0084\3\2\2\2\u0084\u0085\7K\2\2\u0085\u0086\7p\2\2\u0086\u0087\7h\2"+
		"\2\u0087,\3\2\2\2\u0088\u008a\7/\2\2\u0089\u0088\3\2\2\2\u0089\u008a\3"+
		"\2\2\2\u008a\u008b\3\2\2\2\u008b\u008c\5/\30\2\u008c.\3\2\2\2\u008d\u008f"+
		"\5\'\24\2\u008e\u008d\3\2\2\2\u008f\u0090\3\2\2\2\u0090\u008e\3\2\2\2"+
		"\u0090\u0091\3\2\2\2\u0091\u0092\3\2\2\2\u0092\u0094\7\60\2\2\u0093\u0095"+
		"\5\'\24\2\u0094\u0093\3\2\2\2\u0095\u0096\3\2\2\2\u0096\u0094\3\2\2\2"+
		"\u0096\u0097\3\2\2\2\u0097\u0099\3\2\2\2\u0098\u009a\5\61\31\2\u0099\u0098"+
		"\3\2\2\2\u0099\u009a\3\2\2\2\u009a\u00b0\3\2\2\2\u009b\u009d\7\60\2\2"+
		"\u009c\u009e\5\'\24\2\u009d\u009c\3\2\2\2\u009e\u009f\3\2\2\2\u009f\u009d"+
		"\3\2\2\2\u009f\u00a0\3\2\2\2\u00a0\u00a2\3\2\2\2\u00a1\u00a3\5\61\31\2"+
		"\u00a2\u00a1\3\2\2\2\u00a2\u00a3\3\2\2\2\u00a3\u00b0\3\2\2\2\u00a4\u00a5"+
		"\5\'\24\2\u00a5\u00a6\5\61\31\2\u00a6\u00b0\3\2\2\2\u00a7\u00a9\5\'\24"+
		"\2\u00a8\u00a7\3\2\2\2\u00a9\u00aa\3\2\2\2\u00aa\u00a8\3\2\2\2\u00aa\u00ab"+
		"\3\2\2\2\u00ab\u00ad\3\2\2\2\u00ac\u00ae\5\61\31\2\u00ad\u00ac\3\2\2\2"+
		"\u00ad\u00ae\3\2\2\2\u00ae\u00b0\3\2\2\2\u00af\u008e\3\2\2\2\u00af\u009b"+
		"\3\2\2\2\u00af\u00a4\3\2\2\2\u00af\u00a8\3\2\2\2\u00b0\60\3\2\2\2\u00b1"+
		"\u00b3\t\3\2\2\u00b2\u00b4\t\4\2\2\u00b3\u00b2\3\2\2\2\u00b3\u00b4\3\2"+
		"\2\2\u00b4\u00b6\3\2\2\2\u00b5\u00b7\5\'\24\2\u00b6\u00b5\3\2\2\2\u00b7"+
		"\u00b8\3\2\2\2\u00b8\u00b6\3\2\2\2\u00b8\u00b9\3\2\2\2\u00b9\62\3\2\2"+
		"\2\u00ba\u00bb\5\67\34\2\u00bb\64\3\2\2\2\u00bc\u00bd\7\"\2\2\u00bd\66"+
		"\3\2\2\2\u00be\u00c0\t\5\2\2\u00bf\u00be\3\2\2\2\u00c0\u00c1\3\2\2\2\u00c1"+
		"\u00bf\3\2\2\2\u00c1\u00c2\3\2\2\2\u00c28\3\2\2\2\u00c3\u00c7\7)\2\2\u00c4"+
		"\u00c6\5;\36\2\u00c5\u00c4\3\2\2\2\u00c6\u00c9\3\2\2\2\u00c7\u00c5\3\2"+
		"\2\2\u00c7\u00c8\3\2\2\2\u00c8\u00ca\3\2\2\2\u00c9\u00c7\3\2\2\2\u00ca"+
		"\u00cb\7)\2\2\u00cb:\3\2\2\2\u00cc\u00cf\t\6\2\2\u00cd\u00cf\5=\37\2\u00ce"+
		"\u00cc\3\2\2\2\u00ce\u00cd\3\2\2\2\u00cf<\3\2\2\2\u00d0\u00d1\7^\2\2\u00d1"+
		"\u00d2\7)\2\2\u00d2>\3\2\2\2\26\2mvy}\u0082\u0089\u0090\u0096\u0099\u009f"+
		"\u00a2\u00aa\u00ad\u00af\u00b3\u00b8\u00c1\u00c7\u00ce\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}