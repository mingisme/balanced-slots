# balanced-slots


suppose there are x slots, I want to assign some keys into these slots.
Each key can be expired for some time.
Regardless any time, I hope the keys are balanced in these slots.


for example, there are 10 slots: 0 - 9. There are 100 keys: A0 - A99.

first of all, put A0 - A49 into the slots. A good assignment is like this: there are 5 keys in each slot.
slot0: A0 - A4
slot1: A5 - A9
...
slot9: A45 - A49

after some seconds, suppose all keys in slot1 are expired.
Now if I want to put A50 - A54 into the slots, the good assignment is put them into slot1, because no key in this slot.


So from the external view, when I want to put some key into slots, I should always find the best slot.
This is the data structure - balanced-slots for this project.

One more thing, if slots are expanded, before return the best slot, expand this data structure.

