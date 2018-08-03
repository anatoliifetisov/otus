import errno
import numpy as np
import torch
import codecs
import zipfile
import pickle
import shutil

import torch.utils.data as data

from six.moves import urllib
from os import listdir, makedirs, unlink, rmdir
from os.path import isfile, join, expanduser, exists

def download_url(url, root, filename, md5):
    root = expanduser(root)
    fpath = join(root, filename)

    try:
        makedirs(root)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise

    if isfile(fpath) and check_integrity(fpath, md5):
        print("Using downloaded and verified file: " + fpath)
    else:
        try:
            print("Downloading " + url + " to " + fpath)
            urllib.request.urlretrieve(url, fpath)
        except:
            if url[:5] == "https":
                url = url.replace("https:", "http:")
                print("Failed download. Trying https -> http instead."
                      " Downloading " + url + " to " + fpath)
                urllib.request.urlretrieve(url, fpath)
                

class Wikitext_2(data.Dataset):

    url = "https://s3.amazonaws.com/research.metamind.io/wikitext/wikitext-2-raw-v1.zip"
    
    raw_folder = "raw"
    processed_folder = "processed"
    extracted_folder = "wikitext-2-raw"
    
    training_file = "training.pt"
    test_file = "test.pt"
    valid_file = "valid.pt"
    vocabulary_file = "vocabulary.pkl"
    inverse_vocabulary_file = "inverse_vocabulary.pkl"
    
    training_raw = "wiki.train.raw"
    test_raw = "wiki.test.raw"
    valid_raw = "wiki.valid.raw"

    def __init__(self, root, seq_len=1, train=False, test=False, valid=False, transform=None, target_transform=None, download=False):
        def reshape(data):
            x, y = data
            assert len(x) == len(y)
            lines = len(x) // self.seq_len
            x = x.long().narrow(0, 0, lines * self.seq_len).view(self.seq_len, -1).t()
            y = y.long().narrow(0, 0, lines * self.seq_len).view(self.seq_len, -1).t()
            return x, y            
        
        if sum([train, test, valid]) != 1:
            raise RuntimeError("train, test and valid flags are mutually exclusive, raise exactly one of them")        
        
        self.root = expanduser(root)
        self.transform = transform
        self.target_transform = target_transform
        
        self.train = train
        self.test = test
        self.valid = valid
        self.seq_len = seq_len

        if download:
            self.download()

        if not self._check_exists():
            raise RuntimeError("Dataset not found. You can use download=True to download it")
            
        with open(join(self.processed_folder, self.vocabulary_file), "rb") as f:
            self.vocabulary = pickle.load(f)
            
        with open(join(self.processed_folder, self.inverse_vocabulary_file), "rb") as f:
            self.inverse_vocabulary = pickle.load(f)

        if self.train:
            loaded = torch.load(join(self.root, self.processed_folder, self.training_file))
            self.train_data, self.train_labels = reshape(loaded)
        elif self.test:
            loaded = torch.load(join(self.root, self.processed_folder, self.test_file))
            self.test_data, self.test_labels = reshape(loaded)
        else:
            loaded = torch.load(join(self.root, self.processed_folder, self.valid_file))
            self.valid_data, self.valid_labels = reshape(loaded)          
        
           
    def __getitem__(self, index):
        if self.train:
            letter, target = self.train_data[index], self.train_labels[index]
        elif self.test:
            letter, target = self.test_data[index], self.test_labels[index]
        else:
            letter, target = self.valid_data[index], self.valid_data[index]

        if self.transform is not None:
            letter = self.transform(letter)

        if self.target_transform is not None:
            target = self.target_transform(target)

        return letter.reshape(-1), target.reshape(-1)

    def __len__(self):
        if self.train:
            return len(self.train_data)
        elif self.test:
            return len(self.test_data)
        else:
            return len(self.valid_data)

               
    def _check_exists(self):
        return exists(join(self.root, self.processed_folder, self.training_file)) and \
               exists(join(self.root, self.processed_folder, self.test_file)) and \
               exists(join(self.root, self.processed_folder, self.valid_file)) and \
               exists(join(self.root, self.processed_folder, self.vocabulary_file)) and \
               exists(join(self.root, self.processed_folder, self.inverse_vocabulary_file))

               
    def download(self):
        if self._check_exists():
            return
        
        raw = join(self.root, self.raw_folder)
        processed = join(self.root, self.processed_folder)
        extracted = join(raw, self.extracted_folder)
        
        try:
            makedirs(raw)
            makedirs(processed)
        except OSError as e:
            if e.errno == errno.EEXIST:
                pass
            else:
                raise
            

        filename = self.url.rpartition("/")[2]
        file_path = join(raw, filename)
        download_url(self.url, root=raw, filename=filename, md5=None)
        
        with zipfile.ZipFile(file_path, "r") as zip_f:
            zip_f.extractall(raw)
        unlink(file_path)
               
        files = [f for f in listdir(extracted) if isfile(join(extracted, f))]
        for f in files:
            shutil.move(join(extracted, f), raw)
        rmdir(extracted)
         
        files = [join(raw, f) for f in files]
#         files = ["./wikitext/train.txt", "./wikitext/test.txt", "./wikitext/valid.txt"]
        vocabulary = self._build_vocabulary(files)
        
        self.vocabulary = vocabulary
        self.inverse_vocabulary = {v:k for k,v in vocabulary.items()}
        
        train_x = self._encode(vocabulary, join(raw, self.training_raw))
#         train_x = self._encode(vocabulary, "./wikitext/train.txt")
        train_y = train_x[1:]
        train_x = train_x[:-1]
        
        test_x = self._encode(vocabulary, join(raw, self.test_raw))
#         test_x = self._encode(vocabulary, "./wikitext/test.txt")
        test_y = test_x[1:]
        test_x = test_x[:-1]
        
        valid_x = self._encode(vocabulary, join(raw, self.valid_raw))
#         valid_x = self._encode(vocabulary, "./wikitext/valid.txt")
        valid_y = valid_x[1:]
        valid_x = valid_x[:-1]
        
        print("Processing...")
        
        training_set = (torch.Tensor(train_x), torch.Tensor(train_y))
        test_set = (torch.Tensor(test_x), torch.Tensor(test_y))
        valid_set = (torch.Tensor(valid_x), torch.Tensor(valid_y))
        
        with open(join(processed, self.training_file), "wb") as f:
            torch.save(training_set, f)
        with open(join(processed, self.test_file), "wb") as f:
            torch.save(test_set, f)
        with open(join(processed, self.valid_file), "wb") as f:
            torch.save(valid_set, f)
            
        with open(join(processed, self.vocabulary_file), "wb") as f:
            pickle.dump(self.vocabulary, f, pickle.HIGHEST_PROTOCOL)

        with open(join(processed, self.inverse_vocabulary_file), "wb") as f:
            pickle.dump(self.inverse_vocabulary, f, pickle.HIGHEST_PROTOCOL)
            
        print("Done!")
        
               
    def _build_vocabulary(self, files):
        vocabulary = {}        
        index = 0
        for file in files:
            with open(file, "r") as f:
                file_string = f.read()
            
            for c in file_string:         
                if c not in vocabulary:
                    vocabulary[c] = index
                    index += 1

        return vocabulary
    
               
    def _encode(self, vocabulary, file):
        
        with open(file, "r") as f:
            file_string = f.read()
        
        return [vocabulary[c] for c in file_string]

    
    def __repr__(self):
        fmt_str = "Dataset " + self.__class__.__name__ + "\n"
        fmt_str += "    Number of datapoints: {}\n".format(self.__len__())
        tmp = "train" if self.train is True else "test"
        fmt_str += "    Split: {}\n".format(tmp)
        fmt_str += "    Root Location: {}\n".format(self.root)
        tmp = "    Transforms (if any): "
        fmt_str += "{0}{1}\n".format(tmp, self.transform.__repr__().replace("\n", "\n" + " " * len(tmp)))
        tmp = "    Target Transforms (if any): "
        fmt_str += "{0}{1}".format(tmp, self.target_transform.__repr__().replace("\n", "\n" + " " * len(tmp)))
        return fmt_str