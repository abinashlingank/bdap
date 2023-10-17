'4'
MatrixMultiplierDriver.java (Driver Class):
java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
public class MatrixMultiplierDriver {
 public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = Job.getInstance(conf, "Matrix Multiplication");
 job.setJarByClass(MatrixMultiplierDriver.class);
 job.setMapperClass(MatrixMultiplierMapper.class);
 job.setReducerClass(MatrixMultiplierReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(IntWritable.class);
 // Input and Output paths
 Path inputPath = new Path("/user/yourusername/input/");
 Path outputPath = new Path("/user/yourusername/output/");
 // Set input and output paths
 MatrixMultiplierMapper.setInputPath(job, inputPath);
 MatrixMultiplierReducer.setOutputPath(job, outputPath);
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}
• MatrixMultiplierMapper.java (Mapper Class):
java
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplierMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Split the input value (row) into individual values
        String[] values = value.toString().split(","); // Assuming comma-separated values

        // Check for valid input, you may need to handle input validation differently
        if (values.length < 3) {
            // Log or skip invalid input
            return;
        }

        String matrixName = values[0];
        int row = Integer.parseInt(values[1]);
        int column = Integer.parseInt(values[2]);
        int cellValue = Integer.parseInt(values[3]); // Assuming the matrix cell value is at index 3

        if (matrixName.equals("A")) {
            // Emit intermediate key-value pairs for multiplication
            for (int i = 0; i < column; i++) {
                context.write(new Text(row + "," + i), new IntWritable(cellValue));
            }
        } else if (matrixName.equals("B")) {
            // Emit intermediate key-value pairs for multiplication
            for (int i = 0; i < row; i++) {
                context.write(new Text(i + "," + column), new IntWritable(cellValue));
            }
        }
    }
}
• MatrixMultiplierReducer.java (Reducer Class):
java
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMultiplierReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int result = 0;

        // Multiply and sum the values for the same row and column
        for (IntWritable value : values) {
            result += value.get();
        }

        // Emit the result with the same key (row, column)
        context.write(key, new IntWritable(result));
    }
}





'5'

java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
public class WordCountDriver {
 public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = Job.getInstance(conf, "Word Count");
 job.setJarByClass(WordCountDriver.class);
 job.setMapperClass(WordCountMapper.class);
 job.setReducerClass(WordCountReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(IntWritable.class);
 // Input and Output paths
 Path inputPath = new Path("/user/yourusername/input/");
 Path outputPath = new Path("/user/yourusername/output/");
 // Set input and output paths
 WordCountMapper.setInputPath(job, inputPath);
 WordCountReducer.setOutputPath(job, outputPath);
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}
• WordCountMapper.java (Mapper Class):
java
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\\s+"); // Split the line into words using space as a delimiter

        for (String w : words) {
            word.set(w);
            context.write(word, one); // Emit each word with a count of 1
        }
    }
}
• WordCountReducer.java (Reducer Class):
java
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        // Sum up the counts for each word
        for (IntWritable value : values) {
            sum += value.get();
        }

        // Emit the word and its total count
        context.write(key, new IntWritable(sum));
    }
}



'7'

Example 1: Creating a Table and Loading Data
sql
-- Create a table
CREATE TABLE IF NOT EXISTS sample_table (id INT, name STRING);
-- Load data into the table from a local file
LOAD DATA LOCAL INPATH '/path/to/data.txt' INTO TABLE sample_table;
Example 2: Running SQL-like Queries
sql
-- Select all records from the table
SELECT * FROM sample_table;
-- Group data and calculate the average
SELECT name, AVG(id) FROM sample_table GROUP BY name;
-- Filter data
SELECT * FROM sample_table WHERE id > 10;
Example 3: Creating External Tables
sql
-- Create an external table pointing to data in HDFS
CREATE EXTERNAL TABLE IF NOT EXISTS ext_table (id INT, city STRING)
LOCATION '/user/yourusername/hive-data/';
-- Query the external table
SELECT * FROM ext_table;
Example 4: Complex Queries
sql
-- Join two tables
SELECT a.name, b.city
FROM sample_table a
JOIN ext_table b
ON a.id = b.id;
Step 5: Exit Hive
When you're done practicing, exit the Hive shell:
sql
QUIT;



'8'
Example 1: Creating a Table and Inserting Data
# Create a table
create 'student', 'info'
# Insert data
put 'student', '1', 'info:name', 'John'
put 'student', '1', 'info:age', '25'
put 'student', '2', 'info:name', 'Alice'
put 'student', '2', 'info:age', '22'
Example 2: Retrieving Data
# Retrieve data
get 'student', '1'
Example 3: Scanning Data
# Scan all rows in the table
scan 'student'
Step 6: Stop HBase and Thrift
When you're done practicing, stop HBase

'9'

Step 1: Export Data from a MySQL Database
1. Connect to your MySQL database using the MySQL command-line client or a GUI tool.
2. Export a table to a CSV file using the following command:
sql
2. SELECT * INTO OUTFILE '/path/to/output.csv'
3. FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
4. LINES TERMINATED BY '\n'
5. FROM your_table;
Step 2: Import Data into a PostgreSQL Database
1. Connect to your PostgreSQL database using the psql command-line tool or a GUI tool.
2. Create a table with the same schema as the CSV file you want to import.
3. Use the COPY command to import data from the CSV file into the PostgreSQL table:
sql
3. COPY your_table FROM '/path/to/input.csv' CSV HEADER;
4.
Step 3: Export Data from MongoDB
1. Connect to your MongoDB instance using the mongo shell or a GUI tool.
2. Use the mongoexport command to export data from a collection to a JSON or CSV file:
bash
2. mongoexport --db your_db --collection your_collection --out /path/to/output.json
3.
Step 4: Import Data into SQLite
1. Connect to your SQLite database using the sqlite3 command-line tool.
2. Create a table with the same schema as the data you want to import.
3. Use the .import command to import data from a CSV file into the SQLite table:
sql
3. .mode csv
4. .import /path/to/input.csv your_table
5.
Step 5: Verify Data Import and Export
For each database, verify that the data was successfully imported and exported by querying the tables or
collections and comparing the data.
Step 6: Clean Up
After verifying the data import and export, you can clean up by dropping tables or collections as needed and
removing any temporary files created during the process.



 int marks[] = new int[6];int i;
