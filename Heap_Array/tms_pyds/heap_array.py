#!/usr/bin/env python
#=============================================================================================
# Title           :heap_array.py
# Description     :This module contains array implementation of Heap data strcuture.
# Author          :Tanaji Sutar
# Date            :2020-Mar-30
# python_version  :2.7/3
#============================================================================================


import math
import copy

class Heap:
    
    def __init__(self):
        """
        This method initialize the object of heap class.
        """
        self.arr = []

    def hippify(self,arr:list):
        """
        This method push the all elenets in the list to Heap.
        Parameter 
            arr:Array of the numbers to be pushed to heap.
        Return  
            Heap list
        """
        for number in arr:

            self.heapPush(number)
        
        return self.arr

    def heapPush(self,v:int):
        """
        This method push given number to the heap.
        Parameter
            v: (int) A number to be added into the heap
        Return
            True if successfully added to the heap else return False
        """

        # Add at the end 
        self.arr.append(v)
        if len(self.arr) == 1:
            return True
        else:
            pos = len(self.arr)
            while(1):

                if pos == 1:
                    break
                # Get parent
                parent_index  = math.floor(pos/2)
                
                if self.arr[parent_index-1] > self.arr[pos-1]:
                    # Swap
                    self.arr[parent_index-1],self.arr[pos-1] = self.arr[pos-1],self.arr[parent_index-1]
                else:
                    break

                pos = parent_index

        return True

    def heapPop(self):
        """
        This method pop the minimum number from the heap.
        Parameter
        Return
            min_number : (int) 
        """

        # When pop, always remove from top of the three(first element in array) and put last element at the root.
        

        min_number = copy.deepcopy(self.arr[0])
        # Move last element to the root
        self.arr[0] = self.arr[-1]
        del self.arr[-1]

        # Start from root now
        pos = 1

        while(1):

            array_length = len(self.arr)
            left_child_pos = pos + 1
            right_child_pos = pos + 2
            
            if pos == array_length:
                break
            
            # If left exist
            if left_child_pos <= array_length :
                
                # If right exist
                if right_child_pos <=array_length:
                    
                    # If node is less than its child
                    if self.arr[pos-1] > self.arr[left_child_pos-1] or self.arr[pos-1] > self.arr[right_child_pos-1] :
                        
                        # Swap with smallest one
                        if self.arr[left_child_pos-1] > self.arr[right_child_pos-1]:
                            # If right is smaller,swap with right.
                            self.arr[pos-1],self.arr[right_child_pos-1] = self.arr[right_child_pos-1],self.arr[pos-1]
                            pos = right_child_pos

                        else:
                            # Swap with left
                            self.arr[pos-1],self.arr[left_child_pos-1] = self.arr[left_child_pos-1],self.arr[pos-1]
                            pos = left_child_pos
                else:
                    # If right not exist swap with left
                    self.arr[pos-1],self.arr[left_child_pos-1] = self.arr[left_child_pos-1],self.arr[pos-1]
                    pos = left_child_pos

            else:
                # The left and right both does not exist,means we reached at the end.
                break

        return min_number

    def getMin(self):
        """
        This method return minimum number from heap without removing it.
        """    
        return self.arr[0]

    def show(self):
        """
        This method print the heap list.
        """    
        print(self.arr)
        return True
