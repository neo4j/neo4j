/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
// Generated from /Users/mats/gitRoots/3.0-main/neo4j/community/cypher/compatibility-suite/src/main/resources/FeatureResults.g4 by ANTLR 4.5.1
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
		T__9=10, T__10=11, T__11=12, T__12=13, IntegerLiteral=14, DecimalLiteral=15, 
		DIGIT=16, NONZERODIGIT=17, INFINITY=18, FloatingPointLiteral=19, FloatingPointRepr=20, 
		EXPONENTPART=21, SymbolicNameString=22, WS=23, IDENTIFIER=24, StringLiteral=25;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
		"T__9", "T__10", "T__11", "T__12", "IntegerLiteral", "DecimalLiteral", 
		"DIGIT", "NONZERODIGIT", "INFINITY", "FloatingPointLiteral", "FloatingPointRepr", 
		"EXPONENTPART", "SymbolicNameString", "WS", "IDENTIFIER", "StringLiteral", 
		"StringElement", "EscapedSingleQuote"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'('", "')'", "'['", "']'", "'<'", "'>'", "', '", "'true'", "'false'", 
		"'null'", "'{'", "'}'", "':'", null, null, null, null, null, null, null, 
		null, null, "' '"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, "IntegerLiteral", "DecimalLiteral", "DIGIT", "NONZERODIGIT", 
		"INFINITY", "FloatingPointLiteral", "FloatingPointRepr", "EXPONENTPART", 
		"SymbolicNameString", "WS", "IDENTIFIER", "StringLiteral"
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\33\u00c5\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\4\33\t\33\4\34\t\34\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3"+
		"\6\3\6\3\7\3\7\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\n"+
		"\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\r\3\r\3\16\3\16\3\17\5\17`\n\17\3"+
		"\17\3\17\3\20\3\20\3\20\7\20g\n\20\f\20\16\20j\13\20\5\20l\n\20\3\21\3"+
		"\21\5\21p\n\21\3\22\3\22\3\23\5\23u\n\23\3\23\3\23\3\23\3\23\3\24\5\24"+
		"|\n\24\3\24\3\24\3\25\6\25\u0081\n\25\r\25\16\25\u0082\3\25\3\25\6\25"+
		"\u0087\n\25\r\25\16\25\u0088\3\25\5\25\u008c\n\25\3\25\3\25\6\25\u0090"+
		"\n\25\r\25\16\25\u0091\3\25\5\25\u0095\n\25\3\25\3\25\3\25\3\25\6\25\u009b"+
		"\n\25\r\25\16\25\u009c\3\25\5\25\u00a0\n\25\5\25\u00a2\n\25\3\26\3\26"+
		"\5\26\u00a6\n\26\3\26\6\26\u00a9\n\26\r\26\16\26\u00aa\3\27\3\27\3\30"+
		"\3\30\3\31\6\31\u00b2\n\31\r\31\16\31\u00b3\3\32\3\32\7\32\u00b8\n\32"+
		"\f\32\16\32\u00bb\13\32\3\32\3\32\3\33\3\33\5\33\u00c1\n\33\3\34\3\34"+
		"\3\34\2\2\35\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16"+
		"\33\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\2\67"+
		"\2\3\2\7\3\2\63;\4\2GGgg\4\2--//\7\2&&\62;C\\aac|\4\2\2(*\u0201\u00d7"+
		"\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2"+
		"\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2"+
		"\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2"+
		"\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2"+
		"\2\2\61\3\2\2\2\2\63\3\2\2\2\39\3\2\2\2\5;\3\2\2\2\7=\3\2\2\2\t?\3\2\2"+
		"\2\13A\3\2\2\2\rC\3\2\2\2\17E\3\2\2\2\21H\3\2\2\2\23M\3\2\2\2\25S\3\2"+
		"\2\2\27X\3\2\2\2\31Z\3\2\2\2\33\\\3\2\2\2\35_\3\2\2\2\37k\3\2\2\2!o\3"+
		"\2\2\2#q\3\2\2\2%t\3\2\2\2\'{\3\2\2\2)\u00a1\3\2\2\2+\u00a3\3\2\2\2-\u00ac"+
		"\3\2\2\2/\u00ae\3\2\2\2\61\u00b1\3\2\2\2\63\u00b5\3\2\2\2\65\u00c0\3\2"+
		"\2\2\67\u00c2\3\2\2\29:\7*\2\2:\4\3\2\2\2;<\7+\2\2<\6\3\2\2\2=>\7]\2\2"+
		">\b\3\2\2\2?@\7_\2\2@\n\3\2\2\2AB\7>\2\2B\f\3\2\2\2CD\7@\2\2D\16\3\2\2"+
		"\2EF\7.\2\2FG\7\"\2\2G\20\3\2\2\2HI\7v\2\2IJ\7t\2\2JK\7w\2\2KL\7g\2\2"+
		"L\22\3\2\2\2MN\7h\2\2NO\7c\2\2OP\7n\2\2PQ\7u\2\2QR\7g\2\2R\24\3\2\2\2"+
		"ST\7p\2\2TU\7w\2\2UV\7n\2\2VW\7n\2\2W\26\3\2\2\2XY\7}\2\2Y\30\3\2\2\2"+
		"Z[\7\177\2\2[\32\3\2\2\2\\]\7<\2\2]\34\3\2\2\2^`\7/\2\2_^\3\2\2\2_`\3"+
		"\2\2\2`a\3\2\2\2ab\5\37\20\2b\36\3\2\2\2cl\7\62\2\2dh\5#\22\2eg\5!\21"+
		"\2fe\3\2\2\2gj\3\2\2\2hf\3\2\2\2hi\3\2\2\2il\3\2\2\2jh\3\2\2\2kc\3\2\2"+
		"\2kd\3\2\2\2l \3\2\2\2mp\7\62\2\2np\5#\22\2om\3\2\2\2on\3\2\2\2p\"\3\2"+
		"\2\2qr\t\2\2\2r$\3\2\2\2su\7/\2\2ts\3\2\2\2tu\3\2\2\2uv\3\2\2\2vw\7K\2"+
		"\2wx\7p\2\2xy\7h\2\2y&\3\2\2\2z|\7/\2\2{z\3\2\2\2{|\3\2\2\2|}\3\2\2\2"+
		"}~\5)\25\2~(\3\2\2\2\177\u0081\5!\21\2\u0080\177\3\2\2\2\u0081\u0082\3"+
		"\2\2\2\u0082\u0080\3\2\2\2\u0082\u0083\3\2\2\2\u0083\u0084\3\2\2\2\u0084"+
		"\u0086\7\60\2\2\u0085\u0087\5!\21\2\u0086\u0085\3\2\2\2\u0087\u0088\3"+
		"\2\2\2\u0088\u0086\3\2\2\2\u0088\u0089\3\2\2\2\u0089\u008b\3\2\2\2\u008a"+
		"\u008c\5+\26\2\u008b\u008a\3\2\2\2\u008b\u008c\3\2\2\2\u008c\u00a2\3\2"+
		"\2\2\u008d\u008f\7\60\2\2\u008e\u0090\5!\21\2\u008f\u008e\3\2\2\2\u0090"+
		"\u0091\3\2\2\2\u0091\u008f\3\2\2\2\u0091\u0092\3\2\2\2\u0092\u0094\3\2"+
		"\2\2\u0093\u0095\5+\26\2\u0094\u0093\3\2\2\2\u0094\u0095\3\2\2\2\u0095"+
		"\u00a2\3\2\2\2\u0096\u0097\5!\21\2\u0097\u0098\5+\26\2\u0098\u00a2\3\2"+
		"\2\2\u0099\u009b\5!\21\2\u009a\u0099\3\2\2\2\u009b\u009c\3\2\2\2\u009c"+
		"\u009a\3\2\2\2\u009c\u009d\3\2\2\2\u009d\u009f\3\2\2\2\u009e\u00a0\5+"+
		"\26\2\u009f\u009e\3\2\2\2\u009f\u00a0\3\2\2\2\u00a0\u00a2\3\2\2\2\u00a1"+
		"\u0080\3\2\2\2\u00a1\u008d\3\2\2\2\u00a1\u0096\3\2\2\2\u00a1\u009a\3\2"+
		"\2\2\u00a2*\3\2\2\2\u00a3\u00a5\t\3\2\2\u00a4\u00a6\t\4\2\2\u00a5\u00a4"+
		"\3\2\2\2\u00a5\u00a6\3\2\2\2\u00a6\u00a8\3\2\2\2\u00a7\u00a9\5!\21\2\u00a8"+
		"\u00a7\3\2\2\2\u00a9\u00aa\3\2\2\2\u00aa\u00a8\3\2\2\2\u00aa\u00ab\3\2"+
		"\2\2\u00ab,\3\2\2\2\u00ac\u00ad\5\61\31\2\u00ad.\3\2\2\2\u00ae\u00af\7"+
		"\"\2\2\u00af\60\3\2\2\2\u00b0\u00b2\t\5\2\2\u00b1\u00b0\3\2\2\2\u00b2"+
		"\u00b3\3\2\2\2\u00b3\u00b1\3\2\2\2\u00b3\u00b4\3\2\2\2\u00b4\62\3\2\2"+
		"\2\u00b5\u00b9\7)\2\2\u00b6\u00b8\5\65\33\2\u00b7\u00b6\3\2\2\2\u00b8"+
		"\u00bb\3\2\2\2\u00b9\u00b7\3\2\2\2\u00b9\u00ba\3\2\2\2\u00ba\u00bc\3\2"+
		"\2\2\u00bb\u00b9\3\2\2\2\u00bc\u00bd\7)\2\2\u00bd\64\3\2\2\2\u00be\u00c1"+
		"\t\6\2\2\u00bf\u00c1\5\67\34\2\u00c0\u00be\3\2\2\2\u00c0\u00bf\3\2\2\2"+
		"\u00c1\66\3\2\2\2\u00c2\u00c3\7^\2\2\u00c3\u00c4\7)\2\2\u00c48\3\2\2\2"+
		"\26\2_hkot{\u0082\u0088\u008b\u0091\u0094\u009c\u009f\u00a1\u00a5\u00aa"+
		"\u00b3\u00b9\u00c0\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}