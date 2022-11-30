# generate.py

### An application that allows you to create files, merge files (with the ability to delete lines), upload files to a database. Supports the option to calculate the sum of all integers and the median of all fractional numbers in files.

## Installation
Your system must have:

* Python.
* Java 
* MySQL

## Usage

```python generate.py [-h] [-generation ] [-union_files ]  [-delete_row <regexp> ] [-import_file <path>] [-sum_and_median ]```
### Show help message
Use ```-h``` or ```--help``` to get the help message. For example:
``` python generation.py -generation_files```

The utility produces the following output:
```
usage: generate.py [-h] [-generation_files] [-union_files] [-delete_row DELETE_ROW] [-import_file IMPORT_FILE] [-sum_and_median]

options:
  -h, --help            show this help message and exit
  -generation_files     text file generation
  -union_files          merge all files into one
  -delete_row DELETE_ROW
                        deleting from all files a line with a given combination of characters
  -import_file IMPORT_FILE
                        file import procedure
  -sum_and_median       calculating the sum of all integers and the median of all fractional numbers


```
## Examples
### merge all files into one
``` python3 generate.py -union ```


### Delete row with regexp "2020"
```  python3 generate.py -delete_row "2020" ```

### Import file "./files/file_5.csv" to Database
```   python3 generate.py -import_file "./files/file_5.csv" ```

 

