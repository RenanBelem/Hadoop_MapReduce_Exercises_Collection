package basic;

import advanced.customwritable.TipoAnoWritable;
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
/*
        2. (Fácil) O número de transações por tipo de fluxo e ano;
*/
public class Exercicio2 {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        Path input = new Path("in/transactions_amostra.csv");
        Path output = new Path("output/ex2.txt");
        Job j = new Job(c, "TransacoesFlowTipoAno");

        j.setJarByClass(Exercicio2.class);
        j.setMapperClass(MapTransacao.class);
        j.setReducerClass(ReduceTransacao.class);
        j.setCombinerClass(ReduceTransacao.class);

        j.setOutputKeyClass(TipoAnoWritable.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapTransacao extends Mapper<LongWritable, Text, TipoAnoWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String valorCelula[]=value.toString().split(";");
            String ano = value.toString().split(";")[1];
            String fluxo = value.toString().split(";")[4];
            if(!valorCelula[1].equals("country_or_area")){
                con.write(new TipoAnoWritable(ano, fluxo), new IntWritable(1));
            }
        }
    }

    public static class ReduceTransacao extends Reducer<TipoAnoWritable, IntWritable, TipoAnoWritable, IntWritable> {
        public void reduce(TipoAnoWritable word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable i : values){
                soma += i.get();
            }
            con.write(word, new IntWritable(soma));
        }
    }
}
