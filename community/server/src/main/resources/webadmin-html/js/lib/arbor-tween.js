//
//  arbor-tween.js
//  smooth transitions with a realtime clock
//
//  Copyright (c) 2011 Samizdat Drafting Co.
// 
//  Permission is hereby granted, free of charge, to any person
//  obtaining a copy of this software and associated documentation
//  files (the "Software"), to deal in the Software without
//  restriction, including without limitation the rights to use,
//  copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the
//  Software is furnished to do so, subject to the following
//  conditions:
// 
//  The above copyright notice and this permission notice shall be
//  included in all copies or substantial portions of the Software.
// 
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
//  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
//  OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
//  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
//  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
//  WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
//  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
//  OTHER DEALINGS IN THE SOFTWARE.
//

//  Easing Equations in easing.js:
//  Copyright © 2001 Robert Penner. All rights reserved.
//  
//  Open source under the BSD License. Redistribution and use in source
//  and binary forms, with or without modification, are permitted
//  provided that the following conditions are met:
//  
//  Redistributions of source code must retain the above copyright
//  notice, this list of conditions and the following disclaimer.
//  Redistributions in binary form must reproduce the above copyright
//  notice, this list of conditions and the following disclaimer in the
//  documentation and/or other materials provided with the distribution.
//  
//  Neither the name of the author nor the names of contributors may be
//  used to endorse or promote products derived from this software
//  without specific prior written permission.
//  
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
//  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
//  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
//  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
//  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
//  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
//  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
//  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
//  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. 


(function($){

  /*    etc.js */
  //
  // etc.js
  //
  // misc utilities
  //
  
    var trace = function(msg){
      if (typeof(window)=='undefined' || !window.console) return
      var len = arguments.length
      var args = []
      for (var i=0; i<len; i++) args.push(arguments[i])
      try {
        console.log.apply(this, args)
      } catch(e){}
    }  
  
    var dirname = function(path){
      var pth = path.replace(/^\/?(.*?)\/?$/,"$1").split('/')
      pth.pop()
      return "/"+pth.join("/")
    }
    var basename = function(path){
      // var pth = path.replace(/^\//,'').split('/')
      var pth = path.replace(/^\/?(.*?)\/?$/,"$1").split('/')
      
      var base = pth.pop()
      if (base=="") return null
      else return base
    }
  
    var _ordinalize_re = /(\d)(?=(\d\d\d)+(?!\d))/g
    var ordinalize = function(num){
      var norm = ""+num
      if (num < 11000){
        norm = (""+num).replace(_ordinalize_re, "$1,")
      } else if (num < 1000000){
        norm = Math.floor(num/1000)+"k"
      } else if (num < 1000000000){
        norm = (""+Math.floor(num/1000)).replace(_ordinalize_re, "$1,")+"m"
      }
      return norm
    }
  
    /* Nano Templates (Tomasz Mazur, Jacek Becela) */
    var nano = function(template, data){
      return template.replace(/\{([\w\-\.]*)}/g, function(str, key){
        var keys = key.split("."), value = data[keys.shift()]
        $.each(keys, function(){ 
          if (value.hasOwnProperty(this)) value = value[this] 
          else value = str
        })
        return value
      })
    }
    
    var objcopy = function(old){
      if (old===undefined) return undefined
      if (old===null) return null
      
      if (old.parentNode) return old
      switch (typeof old){
        case "string":
        return old.substring(0)
        break
        
        case "number":
        return old + 0
        break
        
        case "boolean":
        return old === true
        break
      }
  
      var newObj = ($.isArray(old)) ? [] : {}
      $.each(old, function(ik, v){
        newObj[ik] = objcopy(v)
      })
      return newObj
    }
    
    var objmerge = function(dst, src){
      dst = dst || {}
      src = src || {}
      var merge = objcopy(dst)
      for (var k in src) merge[k] = src[k]
      return merge
    }
    
    var objcmp = function(a, b, strict_ordering){
      if (!a || !b) return a===b // handle null+undef
      if (typeof a != typeof b) return false // handle type mismatch
      if (typeof a != 'object'){
        // an atomic type
        return a===b
      }else{
        // a collection type
        
        // first compare buckets
        if ($.isArray(a)){
          if (!($.isArray(b))) return false
          if (a.length != b.length) return false
        }else{
          var a_keys = []; for (var k in a) if (a.hasOwnProperty(k)) a_keys.push(k)
          var b_keys = []; for (var k in b) if (b.hasOwnProperty(k)) b_keys.push(k)
          if (!strict_ordering){
            a_keys.sort()
            b_keys.sort()
          }
          if (a_keys.join(',') !== b_keys.join(',')) return false
        }
        
        // then compare contents
        var same = true
        $.each(a, function(ik){
          var diff = objcmp(a[ik], b[ik])
          same = same && diff
          if (!same) return false
        })
        return same
      }
    }
  
    var objkeys = function(obj){
      var keys = []
      $.each(obj, function(k,v){ if (obj.hasOwnProperty(k)) keys.push(k) })
      return keys
    }
    
    var objcontains = function(obj){
      if (!obj || typeof obj!='object') return false
      for (var i=1, j=arguments.length; i<j; i++){
        if (obj.hasOwnProperty(arguments[i])) return true
      }
      return false
    }
  
    var uniq = function(arr){
      // keep in mind that this is only sensible with a list of strings
      // anything else, objkey type coercion will turn it into one anyway
      var len = arr.length
      var set = {}
      for (var i=0; i<len; i++){
        set[arr[i]] = true
      }
  
      return objkeys(set) 
    }
  
    var arbor_path = function(){
      var candidates = $("script").map(function(elt){
        var src = $(this).attr('src')
        if (!src) return
        if (src.match(/arbor[^\/\.]*.js|dev.js/)){
          return src.match(/.*\//) || "/"
        }
      })
  
      if (candidates.length>0) return candidates[0] 
      else return null
    }
    
  
  /* colors.js */
  var Colors = (function(){
    var iscolor_re = /#[0-9a-f]{6}/i
    var hexrgb_re = /#(..)(..)(..)/
  
    var d2h = function(d){
      // decimal to hex
      var s=d.toString(16); 
      return (s.length==2) ? s : '0'+s
    }
    
    var h2d = function(h){
      // hex to decimal
      return parseInt(h,16);
    }
  
    var _isRGB = function(color){
      if (!color || typeof color!='object') return false
      var components = objkeys(color).sort().join("")
      if (components == 'abgr') return true
    }
  
    // var _isHSB = function(color){
    //   if (!color || typeof cssOrHex!='object') return false
    //   var components = objkeys(color).sort().join("")
    //   if (components == 'hsb') return true
    // }
  
  
    var that = {
      CSS:{aliceblue:"#f0f8ff", antiquewhite:"#faebd7", aqua:"#00ffff", aquamarine:"#7fffd4", azure:"#f0ffff", beige:"#f5f5dc", bisque:"#ffe4c4", black:"#000000", blanchedalmond:"#ffebcd", blue:"#0000ff", blueviolet:"#8a2be2", brown:"#a52a2a", burlywood:"#deb887", cadetblue:"#5f9ea0", chartreuse:"#7fff00", chocolate:"#d2691e", coral:"#ff7f50", cornflowerblue:"#6495ed", cornsilk:"#fff8dc", crimson:"#dc143c", cyan:"#00ffff", darkblue:"#00008b", darkcyan:"#008b8b", darkgoldenrod:"#b8860b", darkgray:"#a9a9a9", darkgrey:"#a9a9a9", darkgreen:"#006400", darkkhaki:"#bdb76b", darkmagenta:"#8b008b", darkolivegreen:"#556b2f", darkorange:"#ff8c00", darkorchid:"#9932cc", darkred:"#8b0000", darksalmon:"#e9967a", darkseagreen:"#8fbc8f", darkslateblue:"#483d8b", darkslategray:"#2f4f4f", darkslategrey:"#2f4f4f", darkturquoise:"#00ced1", darkviolet:"#9400d3", deeppink:"#ff1493", deepskyblue:"#00bfff", dimgray:"#696969", dimgrey:"#696969", dodgerblue:"#1e90ff", firebrick:"#b22222", floralwhite:"#fffaf0", forestgreen:"#228b22", fuchsia:"#ff00ff", gainsboro:"#dcdcdc", ghostwhite:"#f8f8ff", gold:"#ffd700", goldenrod:"#daa520", gray:"#808080", grey:"#808080", green:"#008000", greenyellow:"#adff2f", honeydew:"#f0fff0", hotpink:"#ff69b4", indianred:"#cd5c5c", indigo:"#4b0082", ivory:"#fffff0", khaki:"#f0e68c", lavender:"#e6e6fa", lavenderblush:"#fff0f5", lawngreen:"#7cfc00", lemonchiffon:"#fffacd", lightblue:"#add8e6", lightcoral:"#f08080", lightcyan:"#e0ffff", lightgoldenrodyellow:"#fafad2", lightgray:"#d3d3d3", lightgrey:"#d3d3d3", lightgreen:"#90ee90", lightpink:"#ffb6c1", lightsalmon:"#ffa07a", lightseagreen:"#20b2aa", lightskyblue:"#87cefa", lightslategray:"#778899", lightslategrey:"#778899", lightsteelblue:"#b0c4de", lightyellow:"#ffffe0", lime:"#00ff00", limegreen:"#32cd32", linen:"#faf0e6", magenta:"#ff00ff", maroon:"#800000", mediumaquamarine:"#66cdaa", mediumblue:"#0000cd", mediumorchid:"#ba55d3", mediumpurple:"#9370d8", mediumseagreen:"#3cb371", mediumslateblue:"#7b68ee", mediumspringgreen:"#00fa9a", mediumturquoise:"#48d1cc", mediumvioletred:"#c71585", midnightblue:"#191970", mintcream:"#f5fffa", mistyrose:"#ffe4e1", moccasin:"#ffe4b5", navajowhite:"#ffdead", navy:"#000080", oldlace:"#fdf5e6", olive:"#808000", olivedrab:"#6b8e23", orange:"#ffa500", orangered:"#ff4500", orchid:"#da70d6", palegoldenrod:"#eee8aa", palegreen:"#98fb98", paleturquoise:"#afeeee", palevioletred:"#d87093", papayawhip:"#ffefd5", peachpuff:"#ffdab9", peru:"#cd853f", pink:"#ffc0cb", plum:"#dda0dd", powderblue:"#b0e0e6", purple:"#800080", red:"#ff0000", rosybrown:"#bc8f8f", royalblue:"#4169e1", saddlebrown:"#8b4513", salmon:"#fa8072", sandybrown:"#f4a460", seagreen:"#2e8b57", seashell:"#fff5ee", sienna:"#a0522d", silver:"#c0c0c0", skyblue:"#87ceeb", slateblue:"#6a5acd", slategray:"#708090", slategrey:"#708090", snow:"#fffafa", springgreen:"#00ff7f", steelblue:"#4682b4", tan:"#d2b48c", teal:"#008080", thistle:"#d8bfd8", tomato:"#ff6347", turquoise:"#40e0d0", violet:"#ee82ee", wheat:"#f5deb3", white:"#ffffff", whitesmoke:"#f5f5f5", yellow:"#ffff00", yellowgreen:"#9acd32"},
  
      // possible invocations:
      //    decode(1,2,3,.4)      -> {r:1,   g:2,   b:3,   a:0.4}
      //    decode(128, .7)       -> {r:128, g:128, b:128, a:0.7}    
      //    decode("#ff0000")     -> {r:255, g:0,   b:0,   a:1}
      //    decode("#ff0000",.5)  -> {r:255, g:0,   b:0,   a:0.5}
      //    decode("white")       -> {r:255, g:255, b:255, a:1}
      //    decode({r:0,g:0,b:0}) -> {r:0,   g:0,   b:0,   a:1}
      decode:function(clr){
        var argLen = arguments.length
        for (var i=argLen-1; i>=0; i--) if (arguments[i]===undefined) argLen--
        var args = arguments
        if (!clr) return null
        if (argLen==1 && _isRGB(clr)) return clr
  
        var rgb = null
  
        if (typeof clr=='string'){
          var alpha = 1
          if (argLen==2) alpha = args[1]
          
          var nameMatch = that.CSS[clr.toLowerCase()]
          if (nameMatch!==undefined){
             clr = nameMatch
          }
          var hexMatch = clr.match(iscolor_re)
          if (hexMatch){
            vals = clr.match(hexrgb_re)
            // trace(vals)
            if (!vals || !vals.length || vals.length!=4) return null    
            rgb = {r:h2d(vals[1]), g:h2d(vals[2]), b:h2d(vals[3]), a:alpha}
          }
        }else if (typeof clr=='number'){
          if (argLen>=3){
            rgb = {r:args[0], g:args[1], b:args[2], a:1}
            if (argLen>=4) rgb.a *= args[3]
          }else if(argLen>=1){
            rgb = {r:args[0], g:args[0], b:args[0], a:1}
            if (argLen==2) rgb.a *= args[1]
          }
        }
  
  
        // if (!rgb) trace("<null color>")
        // else trace(nano("<r:{r} g:{g} b:{b} a:{a}>",rgb))
        // 
        // if (arguments.length==1){        
        //   if (_isRGB(clr)) return clr
        //   if (!clr || typeof clr!='string') return null
        // 
        //   var nameMatch = that.CSS[clr.toLowerCase()]
        //   if (nameMatch!==undefined){
        //      clr = nameMatch
        //   }
        //   var hexMatch = clr.match(iscolor_re)
        //   if (hexMatch){
        //     vals = clr.match(hexrgb_re)
        //     if (!vals || !vals.length || vals.length!=4) return null    
        //     var rgb = {r:h2d(vals[1]), g:h2d(vals[2]), b:h2d(vals[3])}
        //     return rgb
        //   }
        // }
        
        return rgb
      },
      validate:function(str){
        if (!str || typeof str!='string') return false
        
        if (that.CSS[str.toLowerCase()] !== undefined) return true
        if (str.match(iscolor_re)) return true
        return false
      },
      
      // transform
      mix:function(color1, color2, proportion){
        var c1 = that.decode(color1)
        var c2 = that.decode(color2)
        
        // var mixed = ... should this be a triplet or a string?
      },
      blend:function(rgbOrHex, alpha){
        alpha = (alpha!==undefined) ? Math.max(0,Math.min(1,alpha)) : 1
        
        var rgb = that.decode(rgbOrHex)
        if (!rgb) return null
        
        if (alpha==1) return rgbOrHex
        var rgb = rgbOrHex
        if (typeof rgbOrHex=='string') rgb = that.decode(rgbOrHex)
        
        var blended = objcopy(rgb)
        blended.a *= alpha
        
        return nano("rgba({r},{g},{b},{a})", blended)
      },
      
      // output
      encode:function(rgb){
        if (!_isRGB(rgb)){
          rgb = that.decode(rgb)
          if (!_isRGB(rgb)) return null
        }
        if (rgb.a==1){
          return nano("#{r}{g}{b}", {r:d2h(rgb.r), g:d2h(rgb.g), b:d2h(rgb.b)} )        
        }else{
          return nano("rgba({r},{g},{b},{a})", rgb)
        }
  
        // encoding = encoding || "hex"
        // if (!_isRGB(rgb)) return null
        // switch(encoding){
        // case "hex":
        //   return nano("#{r}{g}{b}", {r:d2h(rgb.r), g:d2h(rgb.g), b:d2h(rgb.b)} )
        //   break
        //   
        // case "rgba":
        //   return nano("rgba({r},{g},{b},{alpha})", rgb)
        //   break
        // }
        // // if (rgb===undefined || !rgb.length || rgb.length!=3) return null
        // // return '#'+$.map(rgb, function(c){return d2h(c)}).join("")
      }
    }
    
    return that
  })()
  
  /* easing.js */
  //
  // easing.js
  // the world-famous penner easing equations
  //
  
  /*
   *
   * TERMS OF USE - EASING EQUATIONS
   * 
   * Open source under the BSD License. 
   * 
   * Copyright © 2001 Robert Penner
   * All rights reserved.
   * 
   * Redistribution and use in source and binary forms, with or without modification, 
   * are permitted provided that the following conditions are met:
   * 
   * Redistributions of source code must retain the above copyright notice, this list of 
   * conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above copyright notice, this list 
   * of conditions and the following disclaimer in the documentation and/or other materials 
   * provided with the distribution.
   * 
   * Neither the name of the author nor the names of contributors may be used to endorse 
   * or promote products derived from this software without specific prior written permission.
   * 
   * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY 
   * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
   * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
   * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
   * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
   * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED 
   * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
   * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED 
   * OF THE POSSIBILITY OF SUCH DAMAGE. 
   *
   */
   
   var Easing = (function(){
    var that = {
      // t: current time, b: beginning value, c: change in value, d: duration   
      linear: function(t, b, c, d){
        return c*(t/d) + b
      },
      quadin: function (t, b, c, d) {
    		return c*(t/=d)*t + b;
    	},
      quadout: function (t, b, c, d) {
    		return -c *(t/=d)*(t-2) + b;
    	},
      quadinout: function (t, b, c, d) {
    		if ((t/=d/2) < 1) return c/2*t*t + b;
    		return -c/2 * ((--t)*(t-2) - 1) + b;
    	},
      cubicin: function (t, b, c, d) {
    		return c*(t/=d)*t*t + b;
    	},
      cubicout: function (t, b, c, d) {
    		return c*((t=t/d-1)*t*t + 1) + b;
    	},
      cubicinout: function (t, b, c, d) {
    		if ((t/=d/2) < 1) return c/2*t*t*t + b;
    		return c/2*((t-=2)*t*t + 2) + b;
    	},
      quartin: function (t, b, c, d) {
    		return c*(t/=d)*t*t*t + b;
    	},
      quartout: function (t, b, c, d) {
    		return -c * ((t=t/d-1)*t*t*t - 1) + b;
    	},
      quartinout: function (t, b, c, d) {
    		if ((t/=d/2) < 1) return c/2*t*t*t*t + b;
    		return -c/2 * ((t-=2)*t*t*t - 2) + b;
    	},
      quintin: function (t, b, c, d) {
    		return c*(t/=d)*t*t*t*t + b;
    	},
      quintout: function (t, b, c, d) {
    		return c*((t=t/d-1)*t*t*t*t + 1) + b;
    	},
      quintinout: function (t, b, c, d) {
    		if ((t/=d/2) < 1) return c/2*t*t*t*t*t + b;
    		return c/2*((t-=2)*t*t*t*t + 2) + b;
    	},
      sinein: function (t, b, c, d) {
    		return -c * Math.cos(t/d * (Math.PI/2)) + c + b;
    	},
      sineout: function (t, b, c, d) {
    		return c * Math.sin(t/d * (Math.PI/2)) + b;
    	},
      sineinout: function (t, b, c, d) {
    		return -c/2 * (Math.cos(Math.PI*t/d) - 1) + b;
    	},
      expoin: function (t, b, c, d) {
    		return (t==0) ? b : c * Math.pow(2, 10 * (t/d - 1)) + b;
    	},
      expoout: function (t, b, c, d) {
    		return (t==d) ? b+c : c * (-Math.pow(2, -10 * t/d) + 1) + b;
    	},
      expoinout: function (t, b, c, d) {
    		if (t==0) return b;
    		if (t==d) return b+c;
    		if ((t/=d/2) < 1) return c/2 * Math.pow(2, 10 * (t - 1)) + b;
    		return c/2 * (-Math.pow(2, -10 * --t) + 2) + b;
    	},
      circin: function (t, b, c, d) {
    		return -c * (Math.sqrt(1 - (t/=d)*t) - 1) + b;
    	},
      circout: function (t, b, c, d) {
    		return c * Math.sqrt(1 - (t=t/d-1)*t) + b;
    	},
      circinout: function (t, b, c, d) {
    		if ((t/=d/2) < 1) return -c/2 * (Math.sqrt(1 - t*t) - 1) + b;
    		return c/2 * (Math.sqrt(1 - (t-=2)*t) + 1) + b;
    	},
      elasticin: function (t, b, c, d) {
    		var s=1.70158;var p=0;var a=c;
    		if (t==0) return b;  if ((t/=d)==1) return b+c;  if (!p) p=d*.3;
    		if (a < Math.abs(c)) { a=c; var s=p/4; }
    		else var s = p/(2*Math.PI) * Math.asin (c/a);
    		return -(a*Math.pow(2,10*(t-=1)) * Math.sin( (t*d-s)*(2*Math.PI)/p )) + b;
    	},
      elasticout: function (t, b, c, d) {
    		var s=1.70158;var p=0;var a=c;
    		if (t==0) return b;  if ((t/=d)==1) return b+c;  if (!p) p=d*.3;
    		if (a < Math.abs(c)) { a=c; var s=p/4; }
    		else var s = p/(2*Math.PI) * Math.asin (c/a);
    		return a*Math.pow(2,-10*t) * Math.sin( (t*d-s)*(2*Math.PI)/p ) + c + b;
    	},
      elasticinout: function (t, b, c, d) {
    		var s=1.70158;var p=0;var a=c;
    		if (t==0) return b;  if ((t/=d/2)==2) return b+c;  if (!p) p=d*(.3*1.5);
    		if (a < Math.abs(c)) { a=c; var s=p/4; }
    		else var s = p/(2*Math.PI) * Math.asin (c/a);
    		if (t < 1) return -.5*(a*Math.pow(2,10*(t-=1)) * Math.sin( (t*d-s)*(2*Math.PI)/p )) + b;
    		return a*Math.pow(2,-10*(t-=1)) * Math.sin( (t*d-s)*(2*Math.PI)/p )*.5 + c + b;
    	},
      backin: function (t, b, c, d, s) {
    		if (s == undefined) s = 1.70158;
    		return c*(t/=d)*t*((s+1)*t - s) + b;
    	},
      backout: function (t, b, c, d, s) {
    		if (s == undefined) s = 1.70158;
    		return c*((t=t/d-1)*t*((s+1)*t + s) + 1) + b;
    	},
      backinout: function (t, b, c, d, s) {
    		if (s == undefined) s = 1.70158; 
    		if ((t/=d/2) < 1) return c/2*(t*t*(((s*=(1.525))+1)*t - s)) + b;
    		return c/2*((t-=2)*t*(((s*=(1.525))+1)*t + s) + 2) + b;
    	},
      bouncein: function (t, b, c, d) {
    		return c - that.bounceOut (d-t, 0, c, d) + b;
    	},
      bounceout: function (t, b, c, d) {
    		if ((t/=d) < (1/2.75)) {
    			return c*(7.5625*t*t) + b;
    		} else if (t < (2/2.75)) {
    			return c*(7.5625*(t-=(1.5/2.75))*t + .75) + b;
    		} else if (t < (2.5/2.75)) {
    			return c*(7.5625*(t-=(2.25/2.75))*t + .9375) + b;
    		} else {
    			return c*(7.5625*(t-=(2.625/2.75))*t + .984375) + b;
    		}
    	},
      bounceinout: function (t, b, c, d) {
    		if (t < d/2) return that.bounceIn (t*2, 0, c, d) * .5 + b;
    		return that.bounceOut(t*2-d, 0, c, d) * .5 + c*.5 + b;
    	}
  	}
  	return that
  })()
  /*  tween.js */
  //
  // tween.js
  //
  // interpolator of .data field members for nodes and edges
  //
  
    var Tween = function(){
      var _tweens = {}
      var _done = true
      
      var that = {
        init:function(){
          return that
        },
        
        busy:function(){
          var busy = false
          for (var k in _tweens){ busy=true; break}
          return busy
        },
        
        to:function(node, dur, to){
          var now = new Date().valueOf()
          var seenFields = {}
  
          var tween = {from:{}, to:{}, colors:{}, node:node, t0:now, t1:now+dur*1000, dur:dur*1000}
          var easing_fn = "linear"
          for (var k in to){
            if (k=='easing'){
              // need to do better here. case insensitive and default to linear
              // also be okay with functions getting passed in
              var ease = to[k].toLowerCase()
              if (ease in Easing) easing_fn = ease
              continue
            }else if (k=='delay'){
              var delay = (to[k]||0) * 1000
              tween.t0 += delay
              tween.t1 += delay
              continue
            }
            
            if (Colors.validate(to[k])){
              // it's a hex color string value
              tween.colors[k] = [Colors.decode(node.data[k]), Colors.decode(to[k]), to[k]]
              seenFields[k] = true
            }else{
              tween.from[k] = (node.data[k]!=undefined) ? node.data[k] : to[k]
              tween.to[k] = to[k]
              seenFields[k] = true
            }
          }
          tween.ease = Easing[easing_fn]
  
          if (_tweens[node._id]===undefined) _tweens[node._id] = []
          _tweens[node._id].push(tween)
          
          // look through queued prunes for any redundancies
          if (_tweens.length>1){
            for (var i=_tweens.length-2; i>=0; i++){
              var tw = _tweens[i]
  
              for (var k in tw.to){
                if (k in seenFields) delete tw.to[k]
                else seenFields[k] = true
              }
  
              for (var k in tw.colors){
                if (k in seenFields) delete tw.colors[k]
                else seenFields[k] = true
              }
  
              if ($.isEmptyObject(tw.colors) && $.isEmptyObject(tw.to)){
                _tweens.splice(i,1)
              }
  
            }
          }
          
          _done = false
        },
  
        interpolate:function(pct, src, dst, ease){
          ease = (ease||"").toLowerCase()
          var easing_fn = Easing.linear
          if (ease in Easing) easing_fn = Easing[ease]
  
          var proportion = easing_fn( pct, 0,1, 1 )
          if (Colors.validate(src) && Colors.validate(dst)){
            return lerpRGB(proportion, src,dst)
          }else if (!isNaN(src)){
            return lerpNumber(proportion, src,dst)
          }else if (typeof src=='string'){
            return (proportion<.5) ? src : dst
          }
          
        },
  
        tick:function(){
          var empty = true
          for (var k in _tweens){ empty=false; break}
          if (empty) return
          
          var now = new Date().valueOf()
          
          $.each(_tweens, function(id, tweens){
            var unprunedTweens = false
            
            $.each(tweens, function(i, tween){
              var proportion = tween.ease( (now-tween.t0), 0,1, tween.dur )
              proportion = Math.min(1.0, proportion)
              var from = tween.from
              var to = tween.to
              var colors = tween.colors
              var nodeData = tween.node.data
  
              var lastTick = (proportion==1.0)
  
              for (var k in to){
                switch (typeof to[k]){
                  case "number":
                    nodeData[k] = lerpNumber(proportion, from[k], to[k])
                    if (k=='alpha') nodeData[k] = Math.max(0,Math.min(1, nodeData[k]))
                    break
                  case "string":
                    if (lastTick){
                      nodeData[k] = to[k]
                    }
                    break
                }
              }
              
              for (var k in colors){
                if (lastTick){
                  nodeData[k] = colors[k][2]
                }else{
                  var rgb = lerpRGB(proportion, colors[k][0], colors[k][1])
                  nodeData[k] = Colors.encode(rgb)
                }
              }
  
              if (lastTick){
                 tween.completed = true
                 unprunedTweens = true
              }
            })
            
            if (unprunedTweens){
              _tweens[id] = $.map(tweens, function(t){ if (!t.completed) return t})
              if (_tweens[id].length==0) delete _tweens[id]
            }
          })
          
          _done = $.isEmptyObject(_tweens)
          return _done
        }
      }
      return that.init()
    }
    
    var lerpNumber = function(proportion,from,to){
      return from + proportion*(to-from)
    }
    
    var lerpRGB = function(proportion,from,to){
      proportion = Math.max(Math.min(proportion,1),0)
      var mixture = {}
      
      $.each('rgba'.split(""), function(i, c){
        mixture[c] = Math.round( from[c] + proportion*(to[c]-from[c]) )
      })
      return mixture
    }
  
    
  // })()

  arbor = (typeof(arbor)!=='undefined') ? arbor : {}
  $.extend(arbor, {
    // not really user-serviceable; use the ParticleSystem’s .tween* methods instead
    Tween:Tween,
    
    // immutable object with useful methods
    colors:{
      CSS:Colors.CSS,           // dictionary: {colorname:#fef2e2,...}
      validate:Colors.validate, // ƒ(str) -> t/f
      decode:Colors.decode,     // ƒ(hexString_or_cssColor) -> {r,g,b,a}
      encode:Colors.encode,     // ƒ({r,g,b,a}) -> hexOrRgbaString
      blend:Colors.blend        // ƒ(color, opacity) -> rgbaString
    }
  })
  
})(this.jQuery)




 
