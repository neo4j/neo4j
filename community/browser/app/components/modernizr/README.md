# Temporary!

This is here only to serve up copies of the most awesome [modernizr][]
dev version through [Bower][Bower], since the current repository is not
Bower friendly, and has issues with long paths that affect cloning on Windows.

The hope is that the original authors will eventually use a smaller distribution
repo through Bower itself, rather than the current [source repository][].

As soon as that happens, this will disappear.

For now, use in `component.json` like this

```javascript
{
  "name": "ClientSide Dependencies",
  "version": "1.0.0",
  "main": "",
  "dependencies": {
    "modernizr": "git+https://github.com/Iristyle/bower-angular.git#2.6.2"
  }
}
```


[modernizr]: http://modernizr.com/
[Bower]: https://github.com/twitter/bower
[source repository]: https://github.com/Modernizr/Modernizr
