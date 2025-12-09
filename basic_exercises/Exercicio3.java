package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import advanced.customwritable.ComdAnoValorWritable;

import java.io.IOException;
/*      3. (Fácil) A média dos valores das commodities por ano;
*/
public class Exercicio3 {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        Path input = new Path("in/transactions_amostra.csv");
        Path output = new Path("output/ex3.txt");
        Job j = new Job(c, "MediaValorComoditeAno");

        j.setJarByClass(Exercicio3.class);
        j.setMapperClass(mapComdPeso.class);
        j.setReducerClass(ReduceComdPeso.class);

        j.setOutputKeyClass(ComdAnoValorWritable.class);
        j.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class mapComdPeso extends Mapper<LongWritable, Text, ComdAnoValorWritable, FloatWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String valorCelula[]=value.toString().split(";");
            final String ano = value.toString().split(";")[1];
            final String comodites = value.toString().split(";")[3];
            final String valor = value.toString().split(";")[5];
            if(!valorCelula[0].equals("country_or_area")){
                con.write(new ComdAnoValorWritable(comodites, ano), new FloatWritable(Float.parseFloat(valor)));
            }
        }
    }

    public static class ReduceComdPeso extends Reducer<ComdAnoValorWritable, FloatWritable, ComdAnoValorWritable, FloatWritable> {
        private int contador = 0;
        public void reduce(ComdAnoValorWritable word, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException {
            int contador = 0;
            float soma = 0;
            for (FloatWritable i : values){
                contador += 1;
                soma += i.get();
            }
            if (this.contador < 5) {
                con.write(word, new FloatWritable(soma / contador));
                this.contador++;
            }
        }
    }
}