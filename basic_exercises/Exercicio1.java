package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
/*      1. (Fácil) O número de transações envolvendo o Brasil;
*/
public class Exercicio1 {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        Path input = new Path("in/transactions_amostra.csv");
        Path output = new Path("output/ex1.txt");
        Job j = new Job(c, "BrasilTransacoesQtd");

        j.setJarByClass(Exercicio1.class);
        j.setMapperClass(MapTransacao.class);
        j.setReducerClass(ReduceTransacao.class);
        j.setCombinerClass(ReduceTransacao.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapTransacao extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            String valorCelula[]=linha.split(";");
            if(!valorCelula[1].equals("country_or_area")){
                if (valorCelula[0].equals("Brazil")) {
                    con.write(new Text(valorCelula[0]), new IntWritable(1));
                }
            }
        }
    }

    public static class ReduceTransacao extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable i : values){
                soma += i.get();
            }
            con.write(word, new IntWritable(soma));
        }
    }
}
