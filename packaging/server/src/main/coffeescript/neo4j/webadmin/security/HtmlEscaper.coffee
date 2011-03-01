
define ['lib/backbone'], () ->
  
  class HtmlEscaper
    
    escape : (text) =>
      return @replaceAll(text, [
        [/&/g,"&amp;"]
        [/</g,"&lt;"]
        [/>/g,"&gt;"]
        [/"/g,"&quot;"]
        [/\ /g,"&nbsp;"]
        [/'/g,"&#x27;"]
        [/\//g,"&#x2F;"]])

    replaceAll : (text, replacements) =>
      for replacement in replacements
        text = text.replace replacement[0], replacement[1]
      return text

