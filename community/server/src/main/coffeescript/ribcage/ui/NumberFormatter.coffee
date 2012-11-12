
define [], () ->
  # Number fancificator
  class NumberFormatter
    
    @fancy : (number) ->
      number = "" + number
      out=[]; i=number.length; pos=0
      while --i >= 0
        if pos++ % 3 is 0
          out.push " "
        out.push number[i]
      out.reverse().join("")

