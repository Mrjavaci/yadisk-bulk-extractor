from os import listdir
import os

mypath = "Z:\E-Books"

for dir in listdir(mypath):
    for file in listdir(mypath + "\\" + dir):
        file_path = mypath + "\\" + dir + "\\" + file
        name_arr = file.split(' - ')
        if len(name_arr) > 2:
            if name_arr[0].lower() == name_arr[1].lower():
                name_arr.pop(0)
                new_file_path = mypath + "\\" + dir + "\\" + " - ".join(name_arr)
                if os.path.exists(new_file_path):
                    os.remove(file_path)
                    print("Silindi: " + file_path)
                else:
                    os.rename(file_path, new_file_path)
                    print("Değişti: " + new_file_path)