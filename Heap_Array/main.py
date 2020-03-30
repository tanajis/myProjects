#!/usr/bin/env python
#=============================================================================================
# Title           :main.py
# Description     :This is driver program.
# Author          :Tanaji Sutar
# Date            :2020-Mar-30
# python_version  :2.7/3
#============================================================================================


from tms_pyds import heap_array

if __name__ == "__main__":

    # Create a min heap using array or list of integers.
    minheap = heap_array.Heap()
    array = [3,2,5,3,7,8]
    minheap.hippify(array)
    
    # Show heap
    minheap.show()

    min_element = minheap.heapPop()
    print('Minimun number using heapPop:%r' % min_element)

    print('Push element t heap')
    minheap.heapPush(-10)
    minheap.show()

    
