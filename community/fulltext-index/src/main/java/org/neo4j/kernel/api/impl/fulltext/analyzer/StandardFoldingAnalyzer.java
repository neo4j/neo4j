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
package org.neo4j.kernel.api.impl.fulltext.analyzer;

import static org.apache.lucene.analysis.en.EnglishAnalyzer.ENGLISH_STOP_WORDS_SET;

import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Analyzer that uses ASCIIFoldingFilter to remove accents (diacritics).
 * Otherwise behaves as standard english analyzer.
 * <p>
 * Note! This analyser may have unexpected behaviour, such as tokenizing,
 * for all non ASCII numbers and symbols such as
 * <pre>
 * ----------------------------
 * ASCII | SYMBOL | DESCRIPTION
 * ----------------------------
 * u00B9 | ¹      | SUPERSCRIPT ONE
 * u00B9 | ¹      | SUPERSCRIPT ONE
 * u2081 | ₁      | SUBSCRIPT ONE
 * u2460 | ①      | CIRCLED DIGIT ONE
 * u24F5 | ⓵      | DOUBLE CIRCLED DIGIT ONE
 * u2776 | ❶      | DINGBAT NEGATIVE CIRCLED DIGIT ONE
 * u2780 | ➀      | DINGBAT CIRCLED SANS-SERIF DIGIT ONE
 * u278A | ➊      | DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT ONE
 * uFF11 | １      | FULLWIDTH DIGIT ONE
 * u2488 | ⒈      | DIGIT ONE FULL STOP
 * u00AB | «       | LEFT-POINTING DOUBLE ANGLE QUOTATION MARK
 * u00BB | »       | RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK
 * u201C | “       | LEFT DOUBLE QUOTATION MARK
 * u201D | ”       | RIGHT DOUBLE QUOTATION MARK
 * u201E | „       | DOUBLE LOW-9 QUOTATION MARK
 * u2033 | ″       | DOUBLE PRIME
 * u2036 | ‶       | REVERSED DOUBLE PRIME
 * u275D | ❝       | HEAVY DOUBLE TURNED COMMA QUOTATION MARK ORNAMENT
 * u275E | ❞       | HEAVY DOUBLE COMMA QUOTATION MARK ORNAMENT
 * u276E | ❮       | HEAVY LEFT-POINTING ANGLE QUOTATION MARK ORNAMENT
 * u276F | ❯       | HEAVY RIGHT-POINTING ANGLE QUOTATION MARK ORNAMENT
 * uFF02 | ＂      | FULLWIDTH QUOTATION MARK
 * u2018 | ‘       | LEFT SINGLE QUOTATION MARK
 * u2019 | ’       | RIGHT SINGLE QUOTATION MARK
 * u201A | ‚       | SINGLE LOW-9 QUOTATION MARK
 * u201B | ‛       | SINGLE HIGH-REVERSED-9 QUOTATION MARK
 * u2032 | ′       | PRIME
 * u2035 | ‵       | REVERSED PRIME
 * u2039 | ‹       | SINGLE LEFT-POINTING ANGLE QUOTATION MARK
 * u203A | ›       | SINGLE RIGHT-POINTING ANGLE QUOTATION MARK
 * u275B | ❛       | HEAVY SINGLE TURNED COMMA QUOTATION MARK ORNAMENT
 * u275C | ❜       | HEAVY SINGLE COMMA QUOTATION MARK ORNAMENT
 * uFF07 | ＇      | FULLWIDTH APOSTROPHE
 * u2010 | ‐       | HYPHEN
 * u2011 | ‑       | NON-BREAKING HYPHEN
 * u2012 | ‒       | FIGURE DASH
 * u2013 | –       | EN DASH
 * u2014 | —       | EM DASH
 * u207B | ⁻       | SUPERSCRIPT MINUS
 * u208B | ₋       | SUBSCRIPT MINUS
 * uFF0D | －      | FULLWIDTH HYPHEN-MINUS
 * u2045 | ⁅       | LEFT SQUARE BRACKET WITH QUILL
 * u2772 | ❲       | LIGHT LEFT TORTOISE SHELL BRACKET ORNAMENT
 * uFF3B | ［      | FULLWIDTH LEFT SQUARE BRACKET
 * u2046 | ⁆       | RIGHT SQUARE BRACKET WITH QUILL
 * u2773 | ❳       | LIGHT RIGHT TORTOISE SHELL BRACKET ORNAMENT
 * uFF3D | ］      | FULLWIDTH RIGHT SQUARE BRACKET
 * u207D | ⁽       | SUPERSCRIPT LEFT PARENTHESIS
 * u208D | ₍       | SUBSCRIPT LEFT PARENTHESIS
 * u2768 | ❨       | MEDIUM LEFT PARENTHESIS ORNAMENT
 * u276A | ❪       | MEDIUM FLATTENED LEFT PARENTHESIS ORNAMENT
 * uFF08 | （      | FULLWIDTH LEFT PARENTHESIS
 * u2E28 | ⸨       | LEFT DOUBLE PARENTHESIS
 * u207E | ⁾       | SUPERSCRIPT RIGHT PARENTHESIS
 * u208E | ₎       | SUBSCRIPT RIGHT PARENTHESIS
 * u2769 | ❩       | MEDIUM RIGHT PARENTHESIS ORNAMENT
 * u276B | ❫       | MEDIUM FLATTENED RIGHT PARENTHESIS ORNAMENT
 * uFF09 | ）      | FULLWIDTH RIGHT PARENTHESIS
 * u2E29 | ⸩       | RIGHT DOUBLE PARENTHESIS
 * u276C | ❬       | MEDIUM LEFT-POINTING ANGLE BRACKET ORNAMENT
 * u2770 | ❰       | HEAVY LEFT-POINTING ANGLE BRACKET ORNAMENT
 * uFF1C | ＜      | FULLWIDTH LESS-THAN SIGN
 * u276D | ❭       | MEDIUM RIGHT-POINTING ANGLE BRACKET ORNAMENT
 * u2771 | ❱       | HEAVY RIGHT-POINTING ANGLE BRACKET ORNAMENT
 * uFF1E | ＞      | FULLWIDTH GREATER-THAN SIGN
 * u2774 | ❴       | MEDIUM LEFT CURLY BRACKET ORNAMENT
 * uFF5B | ｛      | FULLWIDTH LEFT CURLY BRACKET
 * u2775 | ❵       | MEDIUM RIGHT CURLY BRACKET ORNAMENT
 * uFF5D | ｝      | FULLWIDTH RIGHT CURLY BRACKET
 * u207A | ⁺       | SUPERSCRIPT PLUS SIGN
 * u208A | ₊       | SUBSCRIPT PLUS SIGN
 * uFF0B | ＋      | FULLWIDTH PLUS SIGN
 * u207C | ⁼       | SUPERSCRIPT EQUALS SIGN
 * u208C | ₌       | SUBSCRIPT EQUALS SIGN
 * uFF1D | ＝      | FULLWIDTH EQUALS SIGN
 * uFF01 | ！      | FULLWIDTH EXCLAMATION MARK
 * u203C | ‼       | DOUBLE EXCLAMATION MARK
 * u2049 | ⁉       | EXCLAMATION QUESTION MARK
 * uFF03 | ＃      | FULLWIDTH NUMBER SIGN
 * uFF04 | ＄      | FULLWIDTH DOLLAR SIGN
 * u2052 | ⁒       | COMMERCIAL MINUS SIGN
 * uFF05 | ％      | FULLWIDTH PERCENT SIGN
 * uFF06 | ＆      | FULLWIDTH AMPERSAND
 * u204E | ⁎       | LOW ASTERISK
 * uFF0A | ＊      | FULLWIDTH ASTERISK
 * uFF0C | ，      | FULLWIDTH COMMA
 * uFF0E | ．      | FULLWIDTH FULL STOP
 * u2044 | ⁄       | FRACTION SLASH
 * uFF0F | ／      | FULLWIDTH SOLIDUS
 * uFF1A | ：      | FULLWIDTH COLON
 * u204F | ⁏       | REVERSED SEMICOLON
 * uFF1B | ；      | FULLWIDTH SEMICOLON
 * uFF1F | ？      | FULLWIDTH QUESTION MARK
 * u2047 | ⁇       | DOUBLE QUESTION MARK
 * u2048 | ⁈       | QUESTION EXCLAMATION MARK
 * uFF20 | ＠      | FULLWIDTH COMMERCIAL AT
 * uFF3C | ＼      | FULLWIDTH REVERSE SOLIDUS
 * u2038 | ‸       | CARET
 * uFF3E | ＾      | FULLWIDTH CIRCUMFLEX ACCENT
 * uFF3F | ＿      | FULLWIDTH LOW LINE
 * u2053 | ⁓       | SWUNG DASH
 * uFF5E | ～      | FULLWIDTH TILDE
 * </pre>
 *
 * Implementation inspired by org.apache.lucene.analysis.standard.StandardAnalyzer
 */
public final class StandardFoldingAnalyzer extends StopwordAnalyzerBase {
    /** Default maximum allowed token length */
    public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;
    /**
     * All non ascii letters handled by {@link LowerCaseFilter}.
     */
    public static final String NON_ASCII_LETTERS =
            "ÀÁÂÃÄÅĀĂĄƏǍǞǠǺȀȂȦȺᴀḀẠẢẤẦẨẪẬẮẰẲẴẶⒶＡàáâãäåāăąǎǟǡǻȁȃȧɐəɚᶏᶕạảạảấầẩẫậắằẳẵặ"
                    + "ₐₔⓐⱥⱯａꜲÆǢǼᴁꜴꜶꜸꜺꜼ⒜ꜳæǣǽᴂꜵꜷꜹꜻꜽƁƂɃʙᴃḂḄḆⒷＢƀƃɓᵬᶀḃḅḇⓑｂ⒝ÇĆ"
                    + "ĈĊČƇȻʗᴄḈⒸＣçćĉċčƈȼɕḉↄⓒꜾꜿｃ⒞ÐĎĐƉƊƋᴅᴆḊḌḎḐḒⒹꝹＤðďđƌȡɖɗᵭᶁᶑḋḍḏḑḓⓓ"
                    + "ꝺｄǄǱǅǲ⒟ȸǆǳʣʥÈÉÊËĒĔĖĘĚƎƐȄȆȨɆᴇḔḖḘḚḜẸẺẼẾỀỂỄỆⒺⱻＥèéêëēĕėęěǝȅȇȩɇɘɛ"
                    + "ɜɝɞʚᴈᶒᶓᶔḕḗḙḛḝẹẻẽếềểễệₑⓔⱸｅ⒠ƑḞⒻꜰꝻꟻＦƒᵮᶂḟẛⓕꝼｆ⒡ﬀﬃﬄﬁﬂĜĞĠĢƓǤǥǦǧ"
                    + "ǴɢʛḠⒼꝽꝾＧĝğġģǵɠɡᵷᵹᶃḡⓖꝿｇ⒢ĤĦȞʜḢḤḦḨḪⒽⱧⱵＨĥħȟɥɦʮʯḣḥḧḩḫẖⓗⱨⱶｈǶ⒣"
                    + "ƕÌÍÎÏĨĪĬĮİƖƗǏȈȊɪᵻḬḮỈỊⒾꟾＩìíîïĩīĭįıǐȉȋɨᴉᵢᵼᶖḭḯỉịⁱⓘｉĲ⒤ĳĴɈᴊⒿＪĵǰ"
                    + "ȷɉɟʄʝⓙⱼｊ⒥ĶƘǨᴋḰḲḴⓀⱩꝀꝂꝄＫķƙǩʞᶄḱḳḵⓚⱪꝁꝃꝅｋ⒦ĹĻĽĿŁȽʟᴌḶḸḺḼⓁⱠⱢꝆꝈꞀＬ"
                    + "ĺļľŀłƚȴɫɬɭᶅḷḹḻḽⓛⱡꝇꝉꞁｌǇỺǈ⒧ǉỻʪʫƜᴍḾṀṂⓂⱮꟽꟿＭɯɰɱᵯᶆḿṁṃⓜｍ⒨ÑŃŅŇŊƝǸ"
                    + "ȠɴᴎṄṆṈṊⓃＮñńņňŉŋƞǹȵɲɳᵰᶇṅṇṉṋⁿⓝｎǊǋ⒩ǌÒÓÔÕÖØŌŎŐƆƟƠǑǪǬǾȌȎȪȬȮȰᴏᴐṌṎ"
                    + "ṐṒỌỎỐỒỔỖỘỚỜỞỠỢⓄꝊꝌＯòóôõöøōŏőơǒǫǭǿȍȏȫȭȯȱɔɵᴖᴗᶗṍṏṑṓọỏốồổỗộớờởỡợₒ"
                    + "ⓞⱺꝋꝍｏŒɶꝎȢᴕ⒪œᴔꝏȣƤᴘṔṖⓅⱣꝐꝒꝔＰƥᵱᵽᶈṕṗⓟꝑꝓꝕꟼｐ⒫ɊⓆꝖꝘＱĸɋʠⓠꝗꝙ"
                    + "ｑ⒬ȹŔŖŘȒȒɌʀʁᴙᴚṘṚṜṞⓇⱤꝚꞂＲŕŗřȑȓɍɼɽɾɿᵣᵲᵳᶉṙṛṝṟⓡꝛꞃｒ⒭ŚŜŞŠȘṠṢṤṦṨⓈꜱ"
                    + "ꞅＳśŝşšſșȿʂᵴᶊṡṣṥṧṩẜẝⓢꞄｓẞ⒮ßﬆŢŤŦƬƮȚȾᴛṪṬṮṰⓉꞆＴţťŧƫƭțȶʇʈᵵṫṭṯṱẗⓣⱦ"
                    + "ｔÞꝦꜨ⒯ʨþᵺꝧʦꜩÙÚÛÜŨŪŬŮŰŲƯǓǕǗǙǛȔȖɄᴜᵾṲṴṶṸṺỤỦỨỪỬỮỰⓊＵùúûüũūŭůűųưǔ"
                    + "ǖǘǚǜȕȗʉᵤᶙṳṵṷṹṻụủứừửữựⓤｕ⒰ᵫƲɅᴠṼṾỼⓋꝞꝨＶʋʌᵥᶌṽṿⓥⱱⱴꝟｖꝠ⒱ꝡŴǷᴡẀẂẄẆ"
                    + "ẈⓌⱲＷŵƿʍẁẃẅẇẉẘⓦⱳｗ⒲ẊẌⓍＸᶍẋẍₓⓧｘ⒳ÝŶŸƳȲɎʏẎỲỴỶỸỾⓎＹýÿŷƴȳɏʎẏẙỳ"
                    + "ỵỷỹỿⓨｙ⒴ŹŻŽƵȜȤᴢẐẒẔⓏⱫꝢＺźżžƶȝȥɀʐʑᵶᶎẑẓẕⓩⱬꝣｚ⒵";
    /**
     * All non ascii numbers handled by {@link LowerCaseFilter}.
     * This analyzer has undefined behaviour for those numbers but they are
     * kept around for reference.
     */
    public static final String NON_ASCII_NUMBERS = "⁰₀⓪⓿０¹₁①⓵❶➀➊１" + "⒈⑴²₂②⓶❷➁➋２⒉⑵³₃③⓷❸➂➌３⒊⑶⁴₄④⓸❹➃➍４⒋⑷⁵₅⑤⓹❺➄➎５⒌⑸⁶₆"
            + "⑥⓺❻➅➏６⒍⑹⁷₇⑦⓻❼➆➐７⒎⑺⁸₈⑧⓼❽➇➑８⒏⑻⁹₉⑨⓽❾➈➒９⒐⑼⑩⓾❿➉➓"
            + "⒑⑽⑪⓫⒒⑾⑫⓬⒓⑿⑬⓭⒔⒀⑭⓮⒕⒁⑮⓯⒖⒂⑯⓰⒗⒃⑰⓱⒘⒄⑱⓲⒙⒅⑲⓳⒚"
            + "⒆⑳⓴⒛⒇";
    /**
     * All non ascii symbols handled by {@link LowerCaseFilter}
     * This analyzer has undefined behaviour for those symbols but they are
     * kept around for reference.
     */
    public static final String NON_ASCII_SYMBOLS =
            "«»“”„″‶❝❞❮❯＂‘’‚" + "‛′‵‹›❛❜＇‐‑‒–—⁻₋－⁅❲［⁆❳］⁽₍❨❪（⸨⁾₎❩❫）⸩❬❰＜❭❱＞❴｛❵｝⁺₊＋⁼₌＝！‼⁉＃" + "＄⁒％＆⁎＊，．⁄／：⁏；？⁇⁈＠＼‸＾＿⁓～";

    public StandardFoldingAnalyzer() {
        super(ENGLISH_STOP_WORDS_SET);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        StandardTokenizer src = new StandardTokenizer();
        src.setMaxTokenLength(DEFAULT_MAX_TOKEN_LENGTH);
        TokenStream tok = new ASCIIFoldingFilter(src);
        tok = new LowerCaseFilter(tok);
        tok = new StopFilter(tok, stopwords);
        return new TokenStreamComponents(src, tok);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        return new LowerCaseFilter(in);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
