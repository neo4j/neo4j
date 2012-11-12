//
//  arbor-graphics.js
//  canvas fructose
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

(function($){

  /*        etc.js */
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
    
  
  /*     colors.js */
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
  
  /* primitives.js */
  //
  //  primitives
  //
  //  Created by Christian Swinehart on 2010-12-08.
  //  Copyright (c) 2011 Samizdat Drafting Co. All rights reserved.
  //
  
  
  var Primitives = function(ctx, _drawStyle, _fontStyle){
  
      ///MACRO:primitives-start
      var _Oval = function(x,y,w,h,style){
        this.x = x
        this.y = y
        this.w = w
        this.h = h
        this.style = (style!==undefined) ? style : {}
      }
      _Oval.prototype = {
        draw:function(overrideStyle){
          this._draw(overrideStyle)
        },
  
        _draw:function(x,y,w,h, style){
          if (objcontains(x, 'stroke', 'fill', 'width')) style = x
          if (this.x!==undefined){
            x=this.x, y=this.y, w=this.w, h=this.h;
            style = objmerge(this.style, style)
          }
          style = objmerge(_drawStyle, style)
          if (!style.stroke && !style.fill) return
  
          var kappa = .5522848;
              ox = (w / 2) * kappa, // control point offset horizontal
              oy = (h / 2) * kappa, // control point offset vertical
              xe = x + w,           // x-end
              ye = y + h,           // y-end
              xm = x + w / 2,       // x-middle
              ym = y + h / 2;       // y-middle
  
          ctx.save()
            ctx.beginPath();
            ctx.moveTo(x, ym);
            ctx.bezierCurveTo(x, ym - oy, xm - ox, y, xm, y);
            ctx.bezierCurveTo(xm + ox, y, xe, ym - oy, xe, ym);
            ctx.bezierCurveTo(xe, ym + oy, xm + ox, ye, xm, ye);
            ctx.bezierCurveTo(xm - ox, ye, x, ym + oy, x, ym);
            ctx.closePath();
  
            // trace(style.fill, style.stroke)
            if (style.fill!==null){
              // trace("fill",fillColor, Colors.encode(fillColor))
              if (style.alpha!==undefined) ctx.fillStyle = Colors.blend(style.fill, style.alpha)
              else ctx.fillStyle = Colors.encode(style.fill)
              ctx.fill()
            }
  
            if (style.stroke!==null){
              ctx.strokeStyle = Colors.encode(style.stroke)
              if (!isNaN(style.width)) ctx.lineWidth = style.width
              ctx.stroke()
            }      
          ctx.restore()
        }
  
      }
  
      var _Rect = function(x,y,w,h,r,style){
        if (objcontains(r, 'stroke', 'fill', 'width')){
           style = r
           r = 0
        }
        this.x = x
        this.y = y
        this.w = w
        this.h = h
        this.r = (r!==undefined) ? r : 0
        this.style = (style!==undefined) ? style : {}
      }
      _Rect.prototype = {
        draw:function(overrideStyle){
          this._draw(overrideStyle)
        },
  
        _draw:function(x,y,w,h,r, style){
          if (objcontains(r, 'stroke', 'fill', 'width', 'alpha')){
            style = r; r=0;
          }else if (objcontains(x, 'stroke', 'fill', 'width', 'alpha')){
            style = x
          }
          if (this.x!==undefined){
            x=this.x, y=this.y, w=this.w, h=this.h;
            style = objmerge(this.style, style)
          }
          style = objmerge(_drawStyle, style)
          if (!style.stroke && !style.fill) return
  
          var rounded = (r>0)
          ctx.save()
          ctx.beginPath();
          ctx.moveTo(x+r, y);
          ctx.lineTo(x+w-r, y);
          if (rounded) ctx.quadraticCurveTo(x+w, y, x+w, y+r);
          ctx.lineTo(x+w, y+h-r);
          if (rounded) ctx.quadraticCurveTo(x+w, y+h, x+w-r, y+h);
          ctx.lineTo(x+r, y+h);
          if (rounded) ctx.quadraticCurveTo(x, y+h, x, y+h-r);
          ctx.lineTo(x, y+r);
          if (rounded) ctx.quadraticCurveTo(x, y, x+r, y);      
  
  
          if (style.fill!==null){
            if (style.alpha!==undefined) ctx.fillStyle = Colors.blend(style.fill, style.alpha)
            else ctx.fillStyle = Colors.encode(style.fill)
            ctx.fill()
          }
  
          if (style.stroke!==null){
            ctx.strokeStyle = Colors.encode(style.stroke)
            if (!isNaN(style.width)) ctx.lineWidth = style.width
            ctx.stroke()
          }      
          ctx.restore()
        }
      }
  
      var _Path = function(x1, y1, x2, y2, style){
        // calling patterns:
        // ƒ( x1, y1, x2, y2, <style> )
        // ƒ( {x:1, y:1}, {x:2, y:2}, <style> )
        // ƒ( [ {x:1, y:1}, {x:2, y:2}, ...], <style> ) one continuous line
        // ƒ( [ [{x,y}, {x,y}], [{x,y}, {x,y}], ...], <style> ) separate lines
  
        if (style!==undefined || typeof y2=='number'){
          // ƒ( x1, y1, x2, y2, <style> )
          this.points = [ {x:x1,y:y1}, {x:x2,y:y2} ]
          this.style = style || {}
        }else if ($.isArray(x1)){
          // ƒ( [ {x:1, y:1}, {x:2, y:2}, ...], <style> )
          this.points = x1
          this.style = y1 || {}
        }else{
          // ƒ( {x:1, y:1}, {x:2, y:2}, <style> )
          this.points = [ x1, y1 ]
          this.style = x2 || {}
        }
      }
      _Path.prototype = {
        draw:function(overrideStyle){
          if (this.points.length<2) return
  
          var sublines = []
          if (!$.isArray(this.points[0])) sublines.push(this.points)
          else sublines = this.points
          
          ctx.save()
            ctx.beginPath();
            $.each(sublines, function(i, lineseg){
              ctx.moveTo(lineseg[0].x+.5, lineseg[0].y+.5);
              $.each(lineseg, function(i, pt){
                if (i==0) return
                ctx.lineTo(pt.x+.5, pt.y+.5);
              })
            })
  
            var style = $.extend(objmerge(_drawStyle, this.style), overrideStyle)
            if (style.closed) ctx.closePath()
  
            if (style.fill!==undefined){
              var fillColor = Colors.decode(style.fill, (style.alpha!==undefined) ? style.alpha : 1)
              if (fillColor) ctx.fillStyle = Colors.encode(fillColor)
                ctx.fill()
            }
  
            if (style.stroke!==undefined){
              var strokeColor = Colors.decode(style.stroke, (style.alpha!==undefined) ? style.alpha : 1)
              if (strokeColor) ctx.strokeStyle = Colors.encode(strokeColor)
              if (!isNaN(style.width)) ctx.lineWidth = style.width
              ctx.stroke()
            }
    			ctx.restore()
        }
      }
      
  
      var _Color = function(a,b,c,d){
        var rgba = Colors.decode(a,b,c,d)
        if (rgba){
          this.r = rgba.r
          this.g = rgba.g
          this.b = rgba.b
          this.a = rgba.a
        }
      }
  
      _Color.prototype = {
        toString:function(){
          return Colors.encode(this)
        },
        blend:function(){
          trace("blend",this.r,this.g,this.b,this.a)
        }
      }
  
      // var _Font = function(face, size){
      //   this.face = (face!=undefined) ? face : "sans-serif"
      //   this.size = (size!=undefined) ? size : 12
      //   // this.alignment = (opts.alignment!=undefined) ? alignment : "left"
      //   // this.baseline = (opts.baseline!=undefined) ? baseline : "ideographic"
      //   // this.color = (opts.color!=undefined) ? Colors.decode(opts.color) : Colors.decode("black")
      // }
      // _Font.prototype = {
      //   _use:function(face, size){
      //     // var params = $.extend({face:face, size:size}, opts)
      //     // $.each('face size alignment baseline color'.split(" "), function(i, param){
      //     //   if (params[param]!==undefined){
      //     //     if (param=='color') _fontStyle[param] = Colors.decode(params[param])
      //     //     else _fontStyle[param] = params[param]
      //     //   }
      //     // })
      // 
      //     // ctx.textAlign = _fontStyle.alignment
      //     // ctx.textBaseline = _fontStyle.baseline
      //     ctx.font = nano("{size}px {face}", {face:face, size:size})
      //     // trace(ctx.font,face,size)      
      //     // ctx.fillStyle = Colors.encode(_fontStyle)
      //     // _fontStyle = {face:face, size:size, alignment:opts.alignment, baseline:opts.baseline, color:opts.color}
      //   },
      //   use:function(){
      //     ctx.font = nano("{size}px {face}", this)
      //   }
      // }
      // 
      // 
  
    ///MACRO:primitives-end
  
    
  
  
  
  
  
  
    return {
      _Oval:_Oval,
      _Rect:_Rect,
      _Color:_Color,
      _Path:_Path
      // _Frame:Frame
    }
  }
  /*   graphics.js */
  //
  //  graphics.js
  //
  //  Created by Christian Swinehart on 2010-12-07.
  //  Copyright (c) 2011 Samizdat Drafting Co. All rights reserved.
  //
  
  var Graphics = function(canvas){
    var dom = $(canvas)
    var ctx = $(dom).get(0).getContext('2d')
  
    var _bounds = null
  
    var _colorMode = "rgb" // vs hsb
    var _coordMode = "origin" // vs "center"
  
    var _drawLibrary = {}
    var _drawStyle = {background:null, 
                      fill:null, 
                      stroke:null,
                      width:0}
  
    var _fontLibrary = {}
    var _fontStyle = {font:"sans-serif",
                     size:12, 
                     align:"left",
                     color:Colors.decode("black"),
                     alpha:1,
                     baseline:"ideographic"}
  
    var _lineBuffer = [] // calls to .lines sit here until flushed by .drawlines
    
    ///MACRO:primitives-start
    var primitives = Primitives(ctx, _drawStyle, _fontStyle)
    var _Oval = primitives._Oval
    var _Rect = primitives._Rect
    var _Color = primitives._Color
    var _Path = primitives._Path
    ///MACRO:primitives-end    
  
  
    // drawStyle({background:"color" or {r,g,b,a}, 
    //            fill:"color" or {r,g,b,a}, 
    //            stroke:"color" or {r,g,b,a}, 
    //            alpha:<number>, 
    //            weight:<number>})
  
  
  
  
    
    var that = {
      init:function(){
        if (!ctx) return null
        return that
      },
  
      // canvas-wide settings
      size:function(width,height){
        if (!isNaN(width) && !isNaN(height)){
          dom.attr({width:width,height:height})
          
          // if (_drawStyle.fill!==null) that.fill(_drawStyle.fill)
          // if (_drawStyle.stroke!==null) that.stroke(_drawStyle.stroke)
          // that.textStyle(_fontStyle)
          
          // trace(_drawStyle,_fontStyle)
        }
        return {width:dom.attr('width'), height:dom.attr('height')}
      },
  
      clear:function(x,y,w,h){
        if (arguments.length<4){
          x=0; y=0
          w=dom.attr('width')
          h=dom.attr('height')
        }
        
        ctx.clearRect(x,y,w,h)
        if (_drawStyle.background!==null){
          ctx.save()
          ctx.fillStyle = Colors.encode(_drawStyle.background)
          ctx.fillRect(x,y,w,h)
          ctx.restore()
        }
      },
  
      background:function(a,b,c,d){
        if (a==null){
          _drawStyle.background = null
          return null
        }
        
        var fillColor = Colors.decode(a,b,c,d)
        if (fillColor){
          _drawStyle.background = fillColor
          that.clear()
        }
      },
  
  
      // drawing to screen
      noFill:function(){
        _drawStyle.fill = null
      },
      fill:function(a,b,c,d){
        if (arguments.length==0){
          return _drawStyle.fill
        }else if (arguments.length>0){
          var fillColor = Colors.decode(a,b,c,d)
          _drawStyle.fill = fillColor
          ctx.fillStyle = Colors.encode(fillColor)
        }
      },
      
      noStroke:function(){
        _drawStyle.stroke = null
        ctx.strokeStyle = null
      },
      stroke:function(a,b,c,d){
        if (arguments.length==0 && _drawStyle.stroke!==null){
          return _drawStyle.stroke
        }else if (arguments.length>0){
          var strokeColor = Colors.decode(a,b,c,d)
          _drawStyle.stroke = strokeColor
          ctx.strokeStyle = Colors.encode(strokeColor)
        }
      },
      strokeWidth:function(ptsize){
        if (ptsize===undefined) return ctx.lineWidth
        ctx.lineWidth = _drawStyle.width = ptsize
      },
      
      
      
      Color:function(clr){
        return new _Color(clr)
      },
  
  
      // Font:function(fontName, pointSize){
      //   return new _Font(fontName, pointSize)
      // },
      // font:function(fontName, pointSize){
      //   if (fontName!==undefined) _fontStyle.font = fontName
      //   if (pointSize!==undefined) _fontStyle.size = pointSize
      //   ctx.font = nano("{size}px {font}", _fontStyle)
      // },
  
  
      drawStyle:function(style){
        // without arguments, show the current state
        if (arguments.length==0) return objcopy(_drawStyle)
        
        // if this is a ("stylename", {style}) invocation, don't change the current
        // state but add it to the library
        if (arguments.length==2){
          var styleName = arguments[0]
          var styleDef = arguments[1]
          if (typeof styleName=='string' && typeof styleDef=='object'){
            var newStyle = {}
            if (styleDef.color!==undefined){
              var textColor = Colors.decode(styleDef.color)
              if (textColor) newStyle.color = textColor
            }
            $.each('background fill stroke width'.split(' '), function(i, param){
              if (styleDef[param]!==undefined) newStyle[param] = styleDef[param]
            })
            if (!$.isEmptyObject(newStyle)) _drawLibrary[styleName] = newStyle
          }
          return
        }
        
        // if a ("stylename") invocation, load up the selected style
        if (arguments.length==1 && _drawLibrary[arguments[0]]!==undefined){
          style = _drawLibrary[arguments[0]]
        }
              
        // for each of the properties specified, update the canvas state
        if (style.width!==undefined) _drawStyle.width = style.width
        ctx.lineWidth = _drawStyle.width
        
        $.each('background fill stroke',function(i, color){
          if (style[color]!==undefined){
            if (style[color]===null) _drawStyle[color] = null
            else{
              var useColor = Colors.decode(style[color])
              if (useColor) _drawStyle[color] = useColor
            }
          }
        })
        ctx.fillStyle = _drawStyle.fill
        ctx.strokeStyle = _drawStyle.stroke
      },
  
      textStyle:function(style){
        // without arguments, show the current state
        if (arguments.length==0) return objcopy(_fontStyle)
        
        // if this is a ("name", {style}) invocation, don't change the current
        // state but add it to the library
        if (arguments.length==2){
          var styleName = arguments[0]
          var styleDef = arguments[1]
          if (typeof styleName=='string' && typeof styleDef=='object'){
            var newStyle = {}
            if (styleDef.color!==undefined){
              var textColor = Colors.decode(styleDef.color)
              if (textColor) newStyle.color = textColor
            }
            $.each('font size align baseline alpha'.split(' '), function(i, param){
              if (styleDef[param]!==undefined) newStyle[param] = styleDef[param]
            })
            if (!$.isEmptyObject(newStyle)) _fontLibrary[styleName] = newStyle
          }
          return
        }
        
        if (arguments.length==1 && _fontLibrary[arguments[0]]!==undefined){
          style = _fontLibrary[arguments[0]]
        }
              
        if (style.font!==undefined) _fontStyle.font = style.font
        if (style.size!==undefined) _fontStyle.size = style.size
        ctx.font = nano("{size}px {font}", _fontStyle)
  
        if (style.align!==undefined){
           ctx.textAlign = _fontStyle.align = style.align
        }
        if (style.baseline!==undefined){
           ctx.textBaseline = _fontStyle.baseline = style.baseline
        }
  
        if (style.alpha!==undefined) _fontStyle.alpha = style.alpha
        if (style.color!==undefined){
          var textColor = Colors.decode(style.color)
          if (textColor) _fontStyle.color = textColor
        }
        if (_fontStyle.color){
          var textColor = Colors.blend(_fontStyle.color, _fontStyle.alpha)
          if (textColor) ctx.fillStyle = textColor
        }
        // trace(_fontStyle,opts)
      },
  
      text:function(textStr, x, y, opts){ // opts: x,y, color, font, align, baseline, width
        if (arguments.length>=3 && !isNaN(x)){
          opts = opts || {}
          opts.x = x
          opts.y = y
        }else if (arguments.length==2 && typeof(x)=='object'){
          opts = x
        }else{
          opts = opts || {}
        }
  
        var style = objmerge(_fontStyle, opts)
        ctx.save()
          if (style.align!==undefined) ctx.textAlign = style.align
          if (style.baseline!==undefined) ctx.textBaseline = style.baseline
          if (style.font!==undefined && !isNaN(style.size)){
            ctx.font = nano("{size}px {font}", style)
          }
  
          var alpha = (style.alpha!==undefined) ? style.alpha : _fontStyle.alpha
          var color = (style.color!==undefined) ? style.color : _fontStyle.color
          ctx.fillStyle = Colors.blend(color, alpha)
          
          // if (alpha>0) ctx.fillText(textStr, style.x, style.y);        
          if (alpha>0) ctx.fillText(textStr, Math.round(style.x), style.y);        
        ctx.restore()
      },
  
      textWidth:function(textStr, style){ // style: x,y, color, font, align, baseline, width
        style = objmerge(_fontStyle, style||{})
        ctx.save()
          ctx.font = nano("{size}px {font}", style)
          var width = ctx.measureText(textStr).width			  
        ctx.restore()
        return width
      },
      
      // hasFont:function(fontName){
      //   var testTxt = 'H h H a H m H b H u H r H g H e H r H f H o H n H s H t H i H v H'
      //   ctx.save()
      //   ctx.font = '10px sans-serif'
      //   var defaultWidth = ctx.measureText(testTxt).width
      // 
      //   ctx.font = '10px "'+fontName+'"'
      //   var putativeWidth = ctx.measureText(testTxt).width
      //   ctx.restore()
      //   
      //   // var defaultWidth = that.textWidth(testTxt, {font:"Times New Roman", size:120})
      //   // var putativeWidth = that.textWidth(testTxt, {font:fontName, size:120})
      //   trace(defaultWidth,putativeWidth,ctx.font)
      //   // return (putativeWidth!=defaultWidth || fontName=="Times New Roman")
      //   return putativeWidth!=defaultWidth
      // },
      
      
      // shape primitives.
      // classes will return an {x,y,w,h, fill(), stroke()} object without drawing
      // functions will draw the shape based on current stroke/fill state
      Rect:function(x,y,w,h,r,style){
        return new _Rect(x,y,w,h,r,style)
      },
      rect:function(x, y, w, h, r, style){
        _Rect.prototype._draw(x,y,w,h,r,style)
      },
      
      Oval:function(x, y, w, h, style) {
        return new _Oval(x,y,w,h, style)
      },
      oval:function(x, y, w, h, style) {
        style = style || {}
        _Oval.prototype._draw(x,y,w,h, style)
      },
      
      // draw a line immediately
      line:function(x1, y1, x2, y2, style){
        var p = new _Path(x1,y1,x2,y2)
        p.draw(style)
      },
      
      // queue up a line segment to be drawn in a batch by .drawLines
      lines:function(x1, y1, x2, y2){
        if (typeof y2=='number'){
          // ƒ( x1, y1, x2, y2)
          _lineBuffer.push( [ {x:x1,y:y1}, {x:x2,y:y2} ] )
        }else{
          // ƒ( {x:1, y:1}, {x:2, y:2} )
          _lineBuffer.push( [ x1,y1 ] )
        }
      },
      
      // flush the buffered .lines to screen
      drawLines:function(style){
        var p = new _Path(_lineBuffer)
        p.draw(style)
        _lineBuffer = []
      }
      
  
    }
    
    return that.init()    
  }
  
  
  // // helpers for figuring out where to draw arrows
  // var intersect_line_line = function(p1, p2, p3, p4)
  // {
  //  var denom = ((p4.y - p3.y)*(p2.x - p1.x) - (p4.x - p3.x)*(p2.y - p1.y));
  // 
  //  // lines are parallel
  //  if (denom === 0) {
  //    return false;
  //  }
  // 
  //  var ua = ((p4.x - p3.x)*(p1.y - p3.y) - (p4.y - p3.y)*(p1.x - p3.x)) / denom;
  //  var ub = ((p2.x - p1.x)*(p1.y - p3.y) - (p2.y - p1.y)*(p1.x - p3.x)) / denom;
  // 
  //  if (ua < 0 || ua > 1 || ub < 0 || ub > 1) {
  //    return false;
  //  }
  // 
  //  return arbor.Point(p1.x + ua * (p2.x - p1.x), p1.y + ua * (p2.y - p1.y));
  // }
  // 
  // var intersect_line_box = function(p1, p2, p3, w, h)
  // {
  //  var tl = {x: p3.x, y: p3.y};
  //  var tr = {x: p3.x + w, y: p3.y};
  //  var bl = {x: p3.x, y: p3.y + h};
  //  var br = {x: p3.x + w, y: p3.y + h};
  // 
  //  var result;
  //  if (result = intersect_line_line(p1, p2, tl, tr)) { return result; } // top
  //  if (result = intersect_line_line(p1, p2, tr, br)) { return result; } // right
  //  if (result = intersect_line_line(p1, p2, br, bl)) { return result; } // bottom
  //  if (result = intersect_line_line(p1, p2, bl, tl)) { return result; } // left
  // 
  //  return false;
  // }
  

  arbor = (typeof(arbor)!=='undefined') ? arbor : {}
  $.extend(arbor, {
    // object constructor (don't use ‘new’, just call it)
    Graphics:function(ctx){ return Graphics(ctx) },

    // useful methods for dealing with the r/g/b
    colors:{
      CSS:Colors.CSS,           // dict:{colorname:"#fef2e2", ...}
      validate:Colors.validate, // ƒ(str) -> t/f
      decode:Colors.decode,     // ƒ(hexString_or_cssColor) -> {r,g,b,a}
      encode:Colors.encode,     // ƒ({r,g,b,a}) -> hexOrRgbaString
      blend:Colors.blend        // ƒ(color, opacity) -> rgbaString
    }
  })
  
})(this.jQuery)