from solardatatools.dataio import load_cassandra_data

data_folder_path = './tmp_data/'

def save_to_csv(data, prefix, name):
    filename = f"{data_folder_path}{prefix}_{name}.csv"
    data.to_csv(filename, index=True)

# List of sites to download
low_time = ['0022F200152D','TABB01125140','ZT161585000441C0490',]
high_time = ['ZT161685000441C0141','ZT155185000441C0083','ZT170385000441C0521',]
low_quality = ['TADBC1079787','001C4B0008A5','ZT162085000441C0944',]
high_quality = ['TAELC1030985','ZT161585000441C0093','TABGC1043585',]
high_length = ['0022F200F291','TAAI01129193','TACLC1036521',]
clipping = ['TADKC1017489','TAEAC1007532','TAEEC1003466',]

prefixes = ['low_time',  'clipping',]
sites = [low_time, high_time, low_quality, high_quality, clipping]


arrays = [
    (low_time, "low_time"),
    (high_time, "high_time"),
    (low_quality, "low_quality"),
    (high_quality, "high_quality"),
    (high_length, "high_length"),
    (clipping, "clipping")
]


for arr, prefix in arrays:
    for entry in arr:
        print(f"Downloading data: {entry}")
        data = load_cassandra_data(entry)
        print(f"Saving data: {entry}")
        save_to_csv(data, prefix, entry)

