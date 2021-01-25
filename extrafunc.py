import time

class Main:
    def calcthisshit(fileinfo1, fileinfo2):
        list1 = [5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100]
        l2 = "[~~~~~~~~~~~~~~~~~~~~]"
        percent = fileinfo1/fileinfo2*100
        percent = float("{:.2f}".format(percent))
        i = 0
        for x in list1:
            if x > percent:
                i = list1.index(x) + 1
                while i != 0:
                    l2 = list(l2)
                    l2[i] = "#"
                    l2 = Main.listtostring(l2)
                    i -= 1
                break
        if percent > 99:
            l2 = "[####################]"
        l2 = l2, "" + str(percent) + "%"
        return Main.listtostring(l2)

    def listtostring(list):
        str = ""
        for x in list:
            str += x
        return str