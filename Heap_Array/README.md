What Is This?
------------

This is a demontration of unit testing using unitest module of python.
It consist of array implementation of heap data structure.
The tests folder contain the testing script to test this module.

How To Use This
---------------

1.Go to project folder Heap_Array

2.Run the project using below cammand

py main.py

3.Perform unit testing using below cammand

py tests/heap_array_tests.py


# cammand line interface
------------------------

Run all the tests

py -m unittest tests/heap_array_tests.py

py -m unittest tests.heap_array_tests.TestHeapArray

Run specific test method
Give testfolder.moduleName.TestClassname.TestMethod

py -m unittest tests.heap_array_tests.TestHeapArray.test_hippify


# Find more at :https://docs.python.org/3/library/unittest.html
