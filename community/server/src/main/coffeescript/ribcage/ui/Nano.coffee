
define [], () ->
  # Nano Templates (Tomasz Mazur, Jacek Becela)
  class Nano
    
    @pipes :
      'truncate' : (value, args) ->
        maxlen = Number(args[0])
        if maxlen isnt NaN and value.length > maxlen
          return value[0...maxlen]
        return value
    
    @compile : (template, data) ->
      template.replace /\{([\w\-\.\|:]*)}/g, (str, key) ->
        pipes = key.split('|')
        keys = pipes.shift().split(".")
        value = data[keys.shift()]
        for key in keys
          if value? and value.hasOwnProperty(key) then value = value[key] 
          else value = str
        
        # Pipe the value through whatever functions was asked for
        for pipe in pipes
          [name, args] = pipe.split ':'
          args = args.split(',')
          value = Nano.pipes[name](value, args)
        
        value

