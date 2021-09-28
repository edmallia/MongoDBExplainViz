# README

**Needs to be improved**

## Prerequisites
Need to have graphiz installed on the machine. Refer to 
- https://graphviz.readthedocs.io/en/stable/manual.html#installation
- https://www.graphviz.org/download/

## Hints to generate JSON file
Use JSON.stringify(). Example
```
JSON.stringify(db.people.find({name:{$gt: "A"}}).sort({name:1}).explain("executionStats"))
```

## To run
```
pip install graphviz
python3 explain-viz.py <filname>
```
