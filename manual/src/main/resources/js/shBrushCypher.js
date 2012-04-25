/**
 * SyntaxHighlighter
 * http://alexgorbatchev.com/SyntaxHighlighter
 *
 * SyntaxHighlighter is donationware. If you are using it, please donate.
 * http://alexgorbatchev.com/SyntaxHighlighter/donate.html
 *
 * @version
 * 3.0.83 (July 02 2010)
 * 
 * @copyright
 * Copyright (C) 2004-2010 Alex Gorbatchev.
 *
 * @license
 * Dual licensed under the MIT and GPL licenses.
 * 
 * Modified by the Neo4j Team.
 */
;(function()
{
	// CommonJS
	typeof(require) != 'undefined' ? SyntaxHighlighter = require('shCore').SyntaxHighlighter : null;

	function Brush()
	{
        var funcs   =   'avg max min sum count not all any none single length type id nodes relationships collect extract shortestPath filter tail head last coalesce abs round sqrt sign collect type length';

        var keywords =  'node relationship rel start match where return skip limit order by descending desc ascending asc distinct true false in is null cypher create with set delete foreach';

        var operators = 'or and';

		this.regexList = [
			{ regex: SyntaxHighlighter.regexLib.singleLineCComments,	css: 'comments' },		// one line comments
			{ regex: SyntaxHighlighter.regexLib.multiLineDoubleQuotedString,	css: 'string' },			// double quoted strings
			{ regex: SyntaxHighlighter.regexLib.multiLineSingleQuotedString,	css: 'string' },			// single quoted strings
			{ regex: new RegExp(this.getKeywords(funcs), 'gmi'),				css: 'color2' },			// functions
			{ regex: new RegExp(this.getKeywords(operators), 'gmi'),			css: 'color1' },			// operators and such
			{ regex: new RegExp(this.getKeywords(keywords), 'gmi'),			css: 'keyword' }			// keyword
			];
	};

	Brush.prototype	= new SyntaxHighlighter.Highlighter();
	Brush.aliases	= ['cypher'];

	SyntaxHighlighter.brushes.Cypher = Brush;

	// CommonJS
	typeof(exports) != 'undefined' ? exports.Brush = Brush : null;
})();

