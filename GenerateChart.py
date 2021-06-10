import numpy as np
import matplotlib.pyplot as plt

N = 10
v1 = (820, 949, 443, 661, 2870, 386, 1120, 464, 543, 767)

v2 = (925, 866, 400, 628, 2850, 268, 1350, 450, 522, 715)

ind = np.arange(10)  
width = 0.25       

fig, ax = plt.subplots()
rects1 = ax.bar(ind, v1, width, color='g')
rects2 = ax.bar(ind+width, v2, width, color='y')
v3 = (1110, 838, 393, 590, 129, 28, 1340, 425, 506, 716)
rects3 = ax.bar(ind + 2*width, v3, width, color='r')
# add some text for labels, title and axes ticks
ax.set_ylabel('ms')
ax.set_xticks(ind + 2*width / 3)
ax.set_xticklabels(('1N', '2N', '3N', '4N', '5N', '1A', '2A', '3A', '4A', '5A'))

ax.legend((rects1[0],rects2[0], rects3[0]), ('ver1','ver2', 'ver3'))


def autolabel(rects):
    """
    Attach a text label above each bar displaying its height
    """
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                '%d' % int(height),
                ha='center', va='bottom')

plt.savefig('v1_and_v2_and_v3.png')
plt.show()