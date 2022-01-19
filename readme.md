PyNestSlicer

A little utility-type thing that takes some nested data and flattens it into unnested files at the target location of your choice.

External Dependencies
* Pandas
* Pyarrow

Notes
1. Still working through edge cases of JSON because people do silly things with JSON. Bless them.
2. While the shredder adds identifiers that respect that relationship of child objects to their parents, it will not guarantee the order of nested objects on multiple runs. Therefore, if you want to use this for onward processing into an updating relational schema you need to have concrete natural identifiers / keys in each object (at the grain you wish to persist.)
