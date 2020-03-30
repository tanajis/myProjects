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
        param : arr
        return Heap list
        """
        for number in arr:

            self.heapPush(number)
        
        return self.arr

    def heapPush(self,v:int):
        """
        This method push the given number in to the heap.
        param : arr
        return heap list
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
                #get parent
                parent_index  = math.floor(pos/2)
                
                if self.arr[parent_index-1] > self.arr[pos-1]:
                    #swap
                    self.arr[parent_index-1],self.arr[pos-1] = self.arr[pos-1],self.arr[parent_index-1]
                else:
                    break

                pos = parent_index
    
    def heapPop(self):
        """
        This method pop the minimum number from the heap.
        return integer
        """

        #when pop, always remove from top of the three(first element in array)
        #and put last element at the root.
        

        min_number = copy.deepcopy(self.arr[0])
        #move last element to the root
        self.arr[0] = self.arr[-1]
        del self.arr[-1]

        #start from root now
        pos = 1

        while(1):

            array_length = len(self.arr)
            left_child_pos = pos + 1
            right_child_pos = pos + 2
            
            if pos == array_length:
                break
            
            #if left exist
            if left_child_pos <= array_length :
                
                #If right exist
                if right_child_pos <=array_length:
                    
                    #if node is less that its child
                    if self.arr[pos-1] > self.arr[left_child_pos-1] or self.arr[pos-1] > self.arr[right_child_pos-1] :
                        
                        #swap with smallest one
                        if self.arr[left_child_pos-1] > self.arr[right_child_pos-1]:
                            #if right is smaller,swap with right
                            self.arr[pos-1],self.arr[right_child_pos-1] = self.arr[right_child_pos-1],self.arr[pos-1]
                            pos = right_child_pos

                        else:
                            #swap with left
                            self.arr[pos-1],self.arr[left_child_pos-1] = self.arr[left_child_pos-1],self.arr[pos-1]
                            pos = left_child_pos
                else:
                    #if right not exist swap with left
                    self.arr[pos-1],self.arr[left_child_pos-1] = self.arr[left_child_pos-1],self.arr[pos-1]
                    pos = left_child_pos

            else:
                #left and right both does not exist,means we reached at the end.
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
