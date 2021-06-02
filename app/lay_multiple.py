import os
import pandas as pd

def to_float(x):
    try:
        return float(x)
    except:
        print('failed to convert ' + str(x) + ' to float.')
        return 0.0

def list_works(size, sub_list):
    sub_list = list(map(to_float, sub_list))
    # print('checking list for size:' + str(size) + ' ' + str(sub_list) + ' sum:' + str(sum(sub_list[:size])) + ' vs:' + str(size*size))
    if len(sub_list[:size]) == size and sum(sub_list[:size]) < size*size:
        # print('found WORKING list for size:' + str(size) + ' ' + str(sub_list) + ' sum:' + str(sum(sub_list[:size])) + ' vs:' + str(size*size))
        # print('WORKS')
        return sub_list[:size]
    return []

# here we check all lists from the start
def bet_lay_sub_list(sub_list):
    bet_lists = []

    if list_works(2, sub_list):
        bet_lists.append(list_works(2, sub_list))

    if list_works(3, sub_list):
        bet_lists.append(list_works(3, sub_list))
    
    if list_works(4, sub_list):
        bet_lists.append(list_works(4, sub_list))

    if list_works(5, sub_list):
        bet_lists.append(list_works(5, sub_list))

    if list_works(6, sub_list):
        bet_lists.append(list_works(6, sub_list))

    if list_works(7, sub_list):
        bet_lists.append(list_works(7, sub_list))

    if list_works(8, sub_list):
        bet_lists.append(list_works(8, sub_list))

    if list_works(9, sub_list):
        bet_lists.append(list_works(9, sub_list))

    if list_works(10, sub_list):
        bet_lists.append(list_works(10, sub_list))

    if list_works(11, sub_list):
        bet_lists.append(list_works(11, sub_list))

    if list_works(12, sub_list):
        bet_lists.append(list_works(12, sub_list))

    if list_works(13, sub_list):
        bet_lists.append(list_works(13, sub_list))

    if list_works(14, sub_list):
        bet_lists.append(list_works(14, sub_list))

    if list_works(15, sub_list):
        bet_lists.append(list_works(15, sub_list))

    return bet_lists


# Check the BSP list to see if there is 
# a sub-list that meets the requirements for
# a LAY-MULTIPLE bet. If so return that sub-list.
def get_lay_bsp_list(bsp_list):
    bsp_list = bsp_list.split(',')
    # start at 1 and then move thru the list
    # checking each number up to 5 or it will get too exspensive.
    bet_lists = []
    for i in range(len(bsp_list)):
        bet_list = bet_lay_sub_list(bsp_list[i:])
        if bet_list:
            bet_lists.extend(bet_list)
    return len(bet_lists)


def test_lists():
    l1 = [3.76, 3.9, 5.0, 6.55, 14.58, 57.6, 65.0, 205.03]
    print('RESULT: ' + str(get_lay_bsp_list(l1)))

if __name__ == '__main__':
    test_lists()



