import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Pivot {

    public static class ColRowPair implements WritableComparable<ColRowPair> {

        private IntWritable col; // natural key
        private LongWritable row; // composite key

        public ColRowPair() {
            set(new IntWritable(), new LongWritable());
        }
        public ColRowPair(int col, Long row) {
            set(new IntWritable(col), new LongWritable(row));
        }
        private void set(IntWritable col, LongWritable row) {
            this.col = col;
            this.row = row;
        }
        public IntWritable getCol() {
            return col;
        }
        public LongWritable getRow() {
            return row;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            col.write(out);
            row.write(out);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            col.readFields(in);
            row.readFields(in);
        }
        @Override
        public int hashCode() {
            return row.hashCode() * 163 + col.hashCode();
        }
        @Override
        public boolean equals(Object o) {
            if (o instanceof ColRowPair) {
                ColRowPair tp = (ColRowPair) o;
                return col.equals(tp.col) && row.equals(tp.row);
            }
            return false;
        }
        @Override
        public int compareTo(ColRowPair tp) {
            int compareValue = this.col.compareTo(tp.getCol());
            if (compareValue == 0) {
                compareValue = this.row.compareTo(tp.getRow());
            }
            return compareValue;    // sort ascending
        }
    }

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, ColRowPair, Text>{

        //private IntWritable col = new IntWritable();
        private ColRowPair colRow = new ColRowPair();
        private Text element = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            int coltp = 0;
            while (itr.hasMoreTokens()) {
                coltp++;
                colRow.set(new IntWritable(coltp), key);
                element = new Text(itr.nextToken());
                context.write(colRow, element);
            }
        }
    }

    public static class ColPartitioner
            extends Partitioner<ColRowPair, Text> {

        @Override
        public int getPartition(ColRowPair pair,
                                Text text,
                                int numberOfPartitions) {
            // make sure that partitions are non-negative
            return Math.abs(pair.getCol().hashCode() % numberOfPartitions);

        }
    }

    public static class ColGroupingComparator extends WritableComparator {
        public ColGroupingComparator() {
            super(ColRowPair.class,true);
        }
        @Override
        public int compare(WritableComparable tp1, WritableComparable tp2) {
            ColRowPair colrowPair = (ColRowPair) tp1;
            ColRowPair colrowPair2 = (ColRowPair) tp2;
            return colrowPair.getCol().compareTo(colrowPair2.getCol());
        }
    }

    public static class PivotReducer
            extends Reducer<ColRowPair, Text, IntWritable, Text> {
        private Text result = new Text();

        public void reduce(ColRowPair colRow, Iterable<Text> elements,
                           Context context
        ) throws IOException, InterruptedException {
            StringBuilder str = new StringBuilder("");
            for (Text ele : elements) {
                if(str.toString().equals("")){
                    str.append(ele.toString());
                } else {
                    str.append(",").append(ele.toString());
                }
            }
            result.set(str.toString().trim());
            context.write(colRow.col, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pivot");
        job.setJarByClass(Pivot.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setPartitionerClass(ColPartitioner.class);
        job.setGroupingComparatorClass(ColGroupingComparator.class);
        job.setReducerClass(PivotReducer.class);
        job.setMapOutputKeyClass(ColRowPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}