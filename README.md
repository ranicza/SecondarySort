# Secondary sort

- MapReduce job for sorting  all the lines in the dataset first by iPinyouId and then by Timestamp;
- iPinyouId with the biggest amount of site-impression (Stream Id = 1) is founded;
- MRUnit tests for Mapper/Reducer

## Build
``` 
mvn clean install

```
## How to run
```
yarn jar ~/secondary-sort/target/secondary-sort-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.epam.bigdata.q3.task4.secondary_sort.MRJob <path_to_input_file> <path_to_output_file>
```

### Example
```
yarn jar /root/eclipse/workspace/secondary-sort/target/secondary-sort-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.epam.bigdata.q3.task4.secondary_sort.MRJob /tmp/admin/homework4/stream.20130606-ab.txt /tmp/admin/homework4/out.txt
```