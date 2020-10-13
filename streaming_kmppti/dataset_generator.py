import random
import os
import math
import csv

class DataGenerator:
    def __init__(self, dataset_type, num_of_rows, num_of_dim, data_name):
        self.export_path = os.path.join(os.getcwd(), "dataset/")
        self.max_value = 200
        self.distance = 5
        self.attr = {
            "data_name": data_name,
            "dataset_type": dataset_type,
            "num_of_rows": num_of_rows,
            "num_of_dim": num_of_dim
        }
        self.col_name = self.__generate_col_name()
        
    def generate(self):
        result = []
        result.append(self.col_name)
        for i in range(self.attr["num_of_rows"]):
            label = self.attr["data_name"] + "-" + str(i+1)
            new_col = [i+1, label]
            # randomize timestamp in           
            new_col.append(self._randomize())
            # randomize timestamp out        
            new_col.append(self._randomize(new_col[2]))
            # randomize value for each dimension
            new_col = self._generate_value(new_col)
            result.append(new_col)
        self.export(result)
    
    def _generate_value(self, new_col):
        return new_col

    def __generate_col_name(self):
        col_name = ["id", "label", "ts_in", "ts_out"]
        for i in range(self.attr["num_of_dim"]):
            col_name.append("dim_" + str(i))
        return col_name

    def _randomize(self, arg=None):
        rand_a = random.randint(0, self.max_value)
        rand_b = rand_a + random.randint(0, self.max_value/2)
        if not (arg is None):
            rand_b = arg + random.randint(self.max_value/2 - math.sqrt(self.max_value/2), self.max_value/2)
            rand = random.randint(arg, rand_b)
        else:
            rand = random.randint(rand_a, rand_b)
        return rand 

    def export(self, result):
        self._makedirs(self.export_path)
        self.attr["num_of_rows"] = str(self.attr["num_of_rows"])
        self.attr["num_of_dim"] = str(self.attr["num_of_dim"])
        filename = self.export_path + "_".join(["%s" % (value) for (key, value) in self.attr.items()]) + ".csv"
        with open(filename, "w") as output:
            writer = csv.writer(output, lineterminator='\n')
            writer.writerows(result)
        print('Exported successfully on ' + self.export_path)

    def _makedirs(self, path_dir):
        if not os.path.exists(path_dir):
            os.makedirs(path_dir)


class Independent(DataGenerator):
    def __init__(self, dataset_type, num_of_rows, num_of_dim, data_name):
        DataGenerator.__init__(self, dataset_type, num_of_rows, num_of_dim, data_name)
        self.export_path += "ind/"
    
    def _generate_value(self, new_col):
        for j in range(self.attr["num_of_dim"]):
            new_col.append(self._randomize())
        return new_col
    

class AntiCorrelated(DataGenerator):
    def __init__(self, dataset_type, num_of_rows, num_of_dim, data_name):
        DataGenerator.__init__(self, dataset_type, num_of_rows, num_of_dim, data_name)
        self.export_path += "ant/"

    def _generate_value(self, new_col):
        rand = self._randomize()
        select_dim = random.randint(0, self.attr["num_of_dim"] - 1)
        for j in range(self.attr["num_of_dim"]):
            if j == select_dim:
                new_col.append(rand)
            else:
                val_of_other_dim = self.max_value - rand + random.randint(-self.distance, self.distance)
                if val_of_other_dim < 0:
                    new_col.append(0)
                elif val_of_other_dim > self.max_value:
                    new_col.append(self.max_value)
                else:
                    new_col.append(val_of_other_dim)
        return new_col