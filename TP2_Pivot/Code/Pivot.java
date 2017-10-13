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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.lang.Integer.parseInt;


public class Pivot {

    public static class RowElementPair implements WritableComparable<RowElementPair> {
        private LongWritable row;
        private Text element;

        public RowElementPair() {
            set(new LongWritable(), new Text());
        }
        public RowElementPair(Long row, String element) {
            set(new LongWritable(row), new Text(element));
        }
        private void set(LongWritable row, Text element) {
            this.row = row;
            this.element = element;
        }
        public String getElementString() {
            return element.toString();
        }
        @Override
        public void write(DataOutput out) throws IOException {
            row.write(out);
            element.write(out);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            row.readFields(in);
            element.readFields(in);
        }
        @Override//!!!!!!!!!!!!!!!!!!!!!!!!!!!不确定hashCode要怎么写
        public int hashCode() {
            return row.hashCode();// * 163 + element.hashCode();
        }
        @Override
        public boolean equals(Object o) {
            if (o instanceof RowElementPair) {
                RowElementPair tp = (RowElementPair) o;
                return row.equals(tp.row);
            }
            return false;
        }
        @Override
        public int compareTo(RowElementPair tp) {

            return row.compareTo(tp.row);
        }
    }

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, IntWritable, RowElementPair>{

        //private IntWritable col = new IntWritable();
        private RowElementPair word = new RowElementPair();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            IntWritable col = new IntWritable(0);
            while (itr.hasMoreTokens()) {
                col.set(col.get()+1);
                word.set(key, new Text(itr.nextToken()));
                context.write(col, word);
            }
        }
    }

    public static class PivotReducer
            extends Reducer<IntWritable, RowElementPair, IntWritable, Text> {
        private Text result = new Text();

        public void reduce(IntWritable col, Iterable<RowElementPair> elements,
                           Context context
        ) throws IOException, InterruptedException {
            StringBuilder str = new StringBuilder("");
            for (RowElementPair ele : elements) {
                if(str.toString().equals("")){
                    str.insert(0, ele.getElementString());
                    //str.append(ele.getElementString());
                } else {
                    str.insert(0,",").insert(0,ele.getElementString());
                    //str.append("，").append(ele.getElementString());
                }
            }
            result.set(str.toString().trim());
            context.write(col, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pivot");
        job.setJarByClass(Pivot.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(PivotReducer.class);
        job.setReducerClass(PivotReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(RowElementPair.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}