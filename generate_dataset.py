import sys

import streaming_kmppti.dataset_generator as dg

def main():
    dataset_type = sys.argv[1]
    num_of_rows = int(sys.argv[2])
    num_of_dim = int(sys.argv[3])
    data_name = sys.argv[4]

    switcher = {
        "i": dg.Independent,
        "ac": dg.AntiCorrelated
    }
    
    obj = switcher[dataset_type]
    dataset = obj(dataset_type, num_of_rows, num_of_dim, data_name)
    dataset.generate()

if __name__ == "__main__":
    main()