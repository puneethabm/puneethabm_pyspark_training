'''
Python basics

@author: Puneetha B M
'''

def print_data_type_of_element(input):
    print("Print data types of the element={0}".format(type(input).__name__))
   
if __name__ == '__main__':
    input = [1, 2, 3]
    print_data_type_of_element(input)