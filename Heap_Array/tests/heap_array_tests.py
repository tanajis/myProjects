import unittest
import os 
import sys

#add parrent dir to path so that all the modules will be includes
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tms_pyds import heap_array

class TestHeapArray(unittest.TestCase):
    
    def test_hippify(self):
        """
        This method tests hippify method.
        """

        #test with positive numbers
        array = [7,1,8,2,4,9,6]
        heap = heap_array.Heap()
        self.assertEqual(heap.hippify(array) , [1,2,6,7,4,9,8])

        #test with negative numbers
        array = [7,-1,8,2,-4,-9,6]
        heap = heap_array.Heap()
        self.assertEqual(heap.hippify(array) , [-9,-1,-4,7,2,8,6])

    def test_heapPush(self):
        """
        This method tests heapPush method.
        """

        #test positive numbers
        heap = heap_array.Heap()
        heap.heapPush(15)
        self.assertEqual(heap.arr , [15])
        heap.heapPush(2)
        self.assertEqual(heap.arr , [2,15])
        heap.heapPush(27)
        self.assertEqual(heap.arr , [2,15,27])
        heap.heapPush(11)
        self.assertEqual(heap.arr , [2,11,27,15])
        heap.heapPush(11)
        self.assertEqual(heap.arr , [2,11,27,15,11]) 

        #test negative numbers
        heap = heap_array.Heap()
        heap.heapPush(-5)
        self.assertEqual( heap.arr , [-5])
        heap.heapPush(2)
        self.assertEqual(heap.arr , [-5,2])
        heap.heapPush(-27)
        self.assertEqual(heap.arr , [-27,2,-5])
        heap.heapPush(11)
        self.assertEqual(heap.arr , [-27,2,-5,11])
        heap.heapPush(-9)
        self.assertEqual(heap.arr , [-27,-9,-5,11,2]) 

    def test_heapPop(self):
        """
        This method tests heapPop method.
        """
        # Test positive numbers
        heap = heap_array.Heap()
        heap.hippify([7,3,5,2,4,8])
        self.assertEqual(heap.heapPop() , 2)
        self.assertEqual(heap.heapPop() , 3)
        self.assertEqual(heap.heapPop() , 4)
        self.assertEqual(heap.heapPop() , 5)
        self.assertEqual(heap.heapPop() , 7)
        self.assertEqual(heap.heapPop() , 8)


        # Test negative numbers
        heap = heap_array.Heap()
        heap.hippify([7,-3,5,-2,4,-8])
        self.assertEqual(heap.heapPop() , -8)
        self.assertEqual(heap.heapPop() , -3)
        self.assertEqual(heap.heapPop() , -2)
        self.assertEqual(heap.heapPop() , 4)
        
    
    def test_getMin(self):
        """
        This method tests getMin method.
        """
        heap = heap_array.Heap()
        heap.hippify([7,3,5,2,4,8])
        self.assertEqual(heap.getMin() , 2)

        heap = heap_array.Heap()
        heap.hippify([150,120,30,117,876])
        self.assertEqual(heap.getMin() , 30)

        # test negative numbers
        heap = heap_array.Heap()
        heap.hippify([150,-120,300,-117,-876])
        self.assertEqual(heap.getMin() , -876)


if __name__ == "__main__":

    unittest.main()
